FROM golang:1.27.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG APP
RUN CGO_ENABLED=0 go build -o /app ./cmd/${APP}

FROM gcr.io/distroless/static-debian12
COPY --from=build /app /app
USER nonroot

ENTRYPOINT ["/app"]