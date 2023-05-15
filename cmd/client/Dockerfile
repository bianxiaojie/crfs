FROM bxjie/golang

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
WORKDIR /app/cmd/client
RUN go build client.go