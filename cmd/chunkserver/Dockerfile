FROM bxjie/golang

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
WORKDIR /app/cmd/chunkserver
RUN go build chunkserver.go

VOLUME /data