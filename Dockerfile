FROM golang:1.20 as build

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o ./main

FROM alpine:latest  

ENV RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/

WORKDIR /root/

COPY --from=build /app/main .

CMD ["./main"]
