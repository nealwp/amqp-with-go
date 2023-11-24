FROM golang:1.20 as build

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /main

FROM alpine:latest  

WORKDIR /root/

COPY --from=build /app/main .

EXPOSE 5672
EXPOSE 15672

CMD ["./main"]
