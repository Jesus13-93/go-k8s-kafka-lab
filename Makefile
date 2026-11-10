MODULE := HelloProjectWithBalance

proto:
	protoc \
  --go_out=. --go_opt=module=$(MODULE) \
  --go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
  api/protobuf/hello.proto