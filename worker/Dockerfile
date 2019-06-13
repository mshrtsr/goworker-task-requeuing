FROM golang:alpine
ENV GO111MODULE on

RUN apk add -U --no-cache \
    git \
    && rm -rf /var/cache/apk/*

ADD ./ /app
WORKDIR /app
RUN go build -o app .
CMD ["./app"] 