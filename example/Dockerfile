FROM golang:1.15-alpine as builder

RUN apk add --no-cache bash make gcc musl-dev linux-headers git
RUN mkdir /pkg
WORKDIR /pkg
COPY go.mod .
COPY go.sum .

RUN go mod download
COPY ./ .

RUN go build -o /build/kv ./example/kv/

FROM alpine:latest

COPY --from=builder /build/kv /

WORKDIR /
ENTRYPOINT ["/kv"]