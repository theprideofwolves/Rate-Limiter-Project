FROM golang:1.22-alpine

WORKDIR /app

# Copy the source code first so `go mod tidy` can detect dependencies
COPY . .

# Generate the missing go.sum and download dependencies
RUN go mod download

# Build the application
RUN go build -o server main.go

EXPOSE 8080

CMD ["./server"]

