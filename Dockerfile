FROM golang:1.27.1-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -trimpath -o /out/wagering-api ./cmd/wagering-api

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S wagering \
    && adduser -S -G wagering wagering

COPY --from=builder /out/wagering-api /usr/local/bin/wagering-api

USER wagering
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/wagering-api"]
