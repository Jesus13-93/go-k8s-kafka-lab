# Go-сервисы в Kubernetes: учебный стенд

Стенд на локальном Kubernetes (kind), на котором я руками проверял, как Go-сервисы ведут себя в кластере: балансировка gRPC, выкат без ошибок, CPU-лимиты и задержки, доставка сообщений в Kafka, планы запросов PostgreSQL.

Это не продакшен-код, а набор воспроизводимых экспериментов. В каждом разделе: что проверял, как повторить, что получилось и какой из этого вывод.

## Что внутри

| Путь | Что это |
|---|---|
| `cmd/server` | gRPC-сервер: возвращает имя пода, умеет имитировать CPU-работу (`-work`), отдаёт метрики Prometheus на `:2112/metrics`, корректно завершается по SIGTERM |
| `cmd/client` | gRPC-клиент-нагрузчик: три режима подключения (`single`, `per-call`, `rr`), считает, какой под ответил |
| `cmd/producer` | Kafka-продюсер: события заказов с ключом `order-N` |
| `cmd/consumer` | Kafka-консьюмер с ручным коммитом, записью в PostgreSQL и переключаемой идемпотентностью; умеет падать до коммита (`-crash-after`) |
| `api/protobuf` | proto-контракт и сгенерированный код |
| `k8s/` | манифесты: Deployment, Service, headless Service, PDB, ServiceMonitor, Kafka (Strimzi, KRaft), топик, PostgreSQL, конфиг kind на 3 ноды |
| `Dockerfile` | один multi-stage Dockerfile на все бинарники (`--build-arg APP=...`), distroless, non-root |

**Стек:** Go, gRPC, franz-go, pgx/v5, Prometheus client_golang · Kubernetes (kind), Strimzi, Kafka (KRaft), PostgreSQL 17, kube-prometheus-stack.

## Запуск

Нужны Docker, kind, kubectl, helm.

```bash
# кластер: control-plane + 2 worker
kind create cluster --name lab --config k8s/kind-multi.yaml

# образы
docker build --build-arg APP=server   -t hello-server:v4 .
docker build --build-arg APP=client   -t hello-client:v3 .
docker build --build-arg APP=producer -t kafka-producer:v1 .
docker build --build-arg APP=consumer -t kafka-consumer:v2 .
kind load docker-image hello-server:v4 hello-client:v3 kafka-producer:v1 kafka-consumer:v2 --name lab

# gRPC-сервис
kubectl apply -f k8s/hello.yaml -f k8s/pdb.yaml

# мониторинг
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install monitoring prometheus-community/kube-prometheus-stack -n monitoring --create-namespace
kubectl apply -f k8s/servicemonitor.yaml

# Kafka и PostgreSQL
helm repo add strimzi https://strimzi.io/charts/
helm install strimzi strimzi/strimzi-kafka-operator -n kafka --create-namespace
kubectl apply -f k8s/kafka.yaml
kubectl -n kafka wait kafka/my-cluster --for=condition=Ready --timeout=300s
kubectl apply -f k8s/topic.yaml -f k8s/postgres.yaml
```

Клиент запускается подом внутри кластера:

```bash
kubectl run client --rm -it --restart=Never \
  --image=hello-client:v3 --image-pull-policy=Never -- \
  -addr=hello:45001 -mode=single -n=300 -interval=10ms
```

## Эксперименты

### 1. Балансировка gRPC: почему Service не балансирует запросы

Три реплики сервера, клиент делает вызовы и считает, какой под ответил.

| Режим клиента | Адрес | Распределение по подам |
|---|---|---|
| `single` — одно соединение | `hello:45001` (ClusterIP) | 300 / 0 / 0 |
| `per-call` — новое соединение на каждый вызов | `hello:45001` (ClusterIP) | 94 / 95 / 111 |
| `rr` — клиентский `round_robin` | `dns:///hello-headless:45001` (headless) | 101 / 99 / 100 |
| `single` + `MaxConnectionAge=10s` на сервере | `hello:45001` (ClusterIP) | 10 / 22 / 28 |

**Вывод.** ClusterIP балансирует на уровне TCP-соединений (L4), а gRPC держит одно долгое HTTP/2-соединение и шлёт все запросы в него, поэтому весь трафик уходит в один под. Варианты решения: клиентская балансировка через headless Service (клиент получает из DNS адреса всех подов и сам распределяет запросы), L7-прокси или `MaxConnectionAge` на сервере. Последний вариант только перекладывает соединение раз в N секунд: трафик переезжает между подами пачками, а не распределяется.

### 2. Выкат без ошибок

Клиент в режиме `rr` делает 600 вызовов, в это время идёт `kubectl rollout restart deployment hello`.

| Попытка | Ошибок из 600 |
|---|---|
| Исходное состояние | 535 |
| После первых правок | 228 |
| После всех правок (два выката подряд) | 0 |

Что понадобилось:

- **Клиент: `dns.SetMinResolutionInterval(time.Second)`.** По умолчанию gRPC перезапрашивает DNS не чаще раза в 30 секунд. За это время все старые поды успевают исчезнуть, и клиент остаётся со списком мёртвых адресов.
- **Клиент: `grpc.WaitForReady(true)`.** Вызов ждёт готового соединения, а не падает сразу, пока клиент переподключается.
- **Сервер: graceful shutdown** по SIGTERM (`GracefulStop` с таймаутом) и `MaxConnectionAge`, чтобы соединения не жили вечно.
- **Deployment:** новый под сначала становится готовым, потом убирается старый, и поду дают время выйти из балансировки до остановки:

```yaml
strategy:
  rollingUpdate:
    maxUnavailable: 0
    maxSurge: 1
minReadySeconds: 5
# в контейнере:
lifecycle:
  preStop:
    sleep:
      seconds: 5   # встроенный sleep Kubernetes: в distroless-образе нет команды sleep
readinessProbe:
  tcpSocket:
    port: 45001
```

### 3. CPU-лимит, throttling и p99

Сервер тратит 5 мс CPU на запрос (`-work=5ms`), латентность меряется гистограммой `hello_request_duration_seconds` в gRPC-интерсепторе.

| CPU limit | Потребление CPU | Доля throttled-периодов | p50 | p99 |
|---|---|---|---|---|
| `200m` | 0.186 ядра | 64% | ~7 мс | ~58 мс |
| `1` | 0.244 ядра | 0% | — | ~8 мс |

Загрузка CPU ноды в обоих случаях была около 7%.

**Вывод.** Лимит CPU работает через квоты CFS: исчерпал квоту в 100-миллисекундном окне — процесс стоит до следующего окна. Среднее потребление ниже лимита и нода почти свободна, а хвост задержек вырос в 7 раз. Поэтому при росте p99 смотреть надо на throttling пода, а не на загрузку ноды.

Запросы для Prometheus:

```promql
# p99 латентности
histogram_quantile(0.99, sum by (le) (rate(hello_request_duration_seconds_bucket[1m])))

# доля throttled-периодов
rate(container_cpu_cfs_throttled_periods_total{pod=~"hello-.*"}[1m])
  / rate(container_cpu_cfs_periods_total{pod=~"hello-.*"}[1m])
```

### 4. Несколько нод, drain и PodDisruptionBudget

Кластер из трёх нод. Поды сервиса разложились по двум worker-нодам (2 + 1): на control-plane стоит taint. При `kubectl drain` с `PodDisruptionBudget` (`minAvailable: 2`, `k8s/pdb.yaml`) выселение не опускает число готовых подов ниже двух: под с ноды уходит только после того, как замена поднялась на другой ноде.

### 5. Kafka: порядок, consumer groups и доставка at-least-once

Топик `orders`, 3 партиции. Продюсер шлёт по три события на заказ (`создан`, `оплачен`, `отправлен`), ключ сообщения — id заказа.

```bash
kubectl -n kafka run producer --rm -it --restart=Never \
  --image=kafka-producer:v1 --image-pull-policy=Never

kubectl -n kafka run consumer --rm -it --restart=Never \
  --image=kafka-consumer:v2 --image-pull-policy=Never -- \
  -group=demo -idempotent=true -crash-after=7

# состояние группы: закоммиченные offset'ы и lag
kubectl -n kafka exec -it my-cluster-dual-role-0 -- \
  bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group demo
```

**Порядок.** Все события одного заказа попадают в одну партицию и читаются по порядку. Между партициями порядка нет.

**Consumer groups.** Партиции делятся между консьюмерами группы, по одному владельцу на партицию. Четвёртый консьюмер на трёх партициях простаивает. Остановленная группа после запуска дочитывает накопленное с закоммиченного offset'а.

**Падение до коммита.** Консьюмер обрабатывает сообщение (пишет «списание» в PostgreSQL), коммитит offset после пачки. Флаг `-crash-after=7` роняет процесс после семи обработанных сообщений, до коммита.

| Режим | Что в базе после перезапуска |
|---|---|
| `-idempotent=false` | 7 сообщений списаны дважды |
| `-idempotent=true` | 0 повторных списаний; из 70 сообщений в топике (каждое событие отправлялось несколько раз) получилось ровно 15 списаний |

Идемпотентность: в одной транзакции с бизнес-операцией вставляется id события в таблицу `processed_events` (`INSERT ... ON CONFLICT DO NOTHING`); если строка уже была, событие пропускается.

**Вывод.** Коммит после обработки даёт at-least-once: при падении между обработкой и коммитом сообщения придут снова. Коммит до обработки дал бы потерю сообщения. Поэтому консьюмер должен быть идемпотентным, а отметка «обработано» и сама операция — в одной транзакции.

### 6. PostgreSQL: планы запросов

Таблица заказов на 1,2–1,3 млн строк в том же PostgreSQL, планы через `EXPLAIN ANALYZE`.

| Что проверял | Было | Стало |
|---|---|---|
| Поиск по `seller_id` без индекса и с индексом | Seq Scan, 25.9 мс | Bitmap Index Scan, 0.9 мс |
| «Последние 20 оплаченных заказов продавца» у продавца с 200 тыс. заказов | чтение всех его заказов + фильтр + сортировка, 30.8 мс | составной индекс `(seller_id, status, created_at DESC)`: Index Scan без Sort, читает 20 строк |
| `WHERE date(created_at) = ...` против диапазона по колонке | Seq Scan, 55 мс | Bitmap Index Scan, 3.3 мс |
| `LIKE 'user4200%'` при collation `en_US.utf8` | обычный индекс не используется, 150 мс | индекс с `text_pattern_ops`, 1.8 мс |
| Сравнение `int`-колонки с `42.0` | колонка приводится к `numeric`, Seq Scan, 75 мс | параметр того же типа — индекс работает |
| Устаревшая статистика после массовой вставки | оценка 102 строки при реальных 100 тыс., 27 мс | после `ANALYZE` другой план, 12 мс |
| `UPDATE` всех строк таблицы | таблица 185 МБ (мёртвые версии строк, bloat) | после `VACUUM FULL` 114 МБ |

**Выводы.**

- Индекс не используется, если колонка обёрнута в функцию или приводится к другому типу, если условие не по левому префиксу составного индекса, если это `LIKE` без побайтовой сортировки.
- В составном индексе сначала идут колонки с равенством, потом колонка сортировки или диапазона: тогда пропадают `Filter` и `Sort`, а `LIMIT` останавливает чтение.
- Seq Scan не всегда ошибка: при выборке большой доли таблицы он дешевле индекса.
- Расхождение оценки и факта в плане на порядки — признак устаревшей статистики.
- `UPDATE` в PostgreSQL создаёт новую версию строки (MVCC); место от старых версий освобождает `VACUUM`, но файл таблицы он не уменьшает.

<details>
<summary>SQL для воспроизведения</summary>

```sql
CREATE TABLE orders (
    id          bigserial PRIMARY KEY,
    seller_id   int           NOT NULL,
    status      text          NOT NULL,
    created_at  timestamptz   NOT NULL,
    amount      numeric(10,2) NOT NULL
);

INSERT INTO orders (seller_id, status, created_at, amount)
SELECT (random() * 10000)::int,
       (ARRAY['created','paid','shipped','delivered','cancelled'])[1 + (random()*4)::int],
       now() - random() * interval '365 days',
       (random() * 10000)::numeric(10,2)
FROM generate_series(1, 1000000);
ANALYZE orders;

-- без индекса и с индексом
EXPLAIN ANALYZE SELECT * FROM orders WHERE seller_id = 42;
CREATE INDEX orders_seller_id_idx ON orders (seller_id);
EXPLAIN ANALYZE SELECT * FROM orders WHERE seller_id = 42;

-- составной индекс под «последние 20 оплаченных заказов продавца»
CREATE INDEX orders_seller_status_created_idx ON orders (seller_id, status, created_at DESC);
EXPLAIN ANALYZE
SELECT * FROM orders
WHERE seller_id = 42 AND status = 'paid'
ORDER BY created_at DESC LIMIT 20;

-- функция над колонкой против диапазона
CREATE INDEX orders_created_at_idx ON orders (created_at);
EXPLAIN ANALYZE SELECT * FROM orders WHERE date(created_at) = '2026-10-01';
EXPLAIN ANALYZE SELECT * FROM orders
WHERE created_at >= '2026-10-01' AND created_at < '2026-10-02';
```

</details>

## Что дальше

- метрика consumer lag в Prometheus и алерт «консьюмер не успевает»;
- обработка ошибок в консьюмере: ретраи и отдельный топик для необработанных сообщений (DLQ);
- пул воркеров в консьюмере с сохранением порядка по ключу.
