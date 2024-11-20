# Usage

```bash
$ cargo run -- --help

GraphQL websocket client to emulate several connections/subscriptions to a subgraph

Usage: graphql_websocket_client --connections <CONNECTIONS> --url <URL> --subscription-file <SUBSCRIPTION_FILE> --protocol <PROTOCOL>

Options:
  -c, --connections <CONNECTIONS>
          Number of websocket connection
  -u, --url <URL>
          Url to the websocket url (example: ws://localhost:4002/graphql)
  -s, --subscription-file <SUBSCRIPTION_FILE>
          Path to subscription file
  -p, --protocol <PROTOCOL>
          GraphQL websocket protocol [possible values: graphql_ws, graphql_transport_ws]
  -h, --help
          Print help
```

Example of usage if you want to execute 1000 subscriptions on a subgraph:

```bash
$ cargo run -- --connections 1000 --url ws://localhost:4002/graphql --subscription-file ./subscription.graphql --protocol graphql_ws
```