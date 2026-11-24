# Build the harness binary into a distroless image. In the cluster the binary
# runs as the writer (it talks to PostgreSQL with pgx, no psql needed); the
# same binary runs the orchestrator on your machine.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /harness .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /harness /harness
ENTRYPOINT ["/harness"]
