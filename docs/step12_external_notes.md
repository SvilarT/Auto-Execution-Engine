# Step 12 External Broker Notes

## Alpaca documentation findings

- The Alpaca Trading API documentation confirms a distinct **Paper Trading** environment intended for end-to-end real-time simulation without routing live exchange orders.
- The same documentation frames Alpaca as offering both paper and live brokerage accounts, which aligns with the repository architecture requirement that paper and live modes use different provider wiring and credentials.
- A direct attempt to access one authentication page URL returned a 404, so the exact authentication endpoint page still needs verification before coding the adapter.

## Design implication for Step 12

- Alpaca remains a strong candidate for the first real broker adapter because it exposes both paper and live trading environments under one API family.
- Before implementation is finalized, the adapter should verify the concrete authentication header names, order submission endpoint, and environment base URLs from current documentation.

## Verified API details

The current Alpaca authentication documentation states that **live Trading API** calls use the host `api.alpaca.markets`, while **paper Trading API** calls use `paper-api.alpaca.markets`. The same page also states that live and paper credentials are distinct and cannot be interchanged, which matches the repository's mode-isolation design.

The same documentation confirms that direct Trading API authentication may be performed by sending the `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY` headers on each request. That is sufficient for a first trading adapter without introducing an OAuth token workflow.[1]

The current Alpaca order-creation reference shows that order submission is performed with `POST /v2/orders`, and its example base URL is `https://paper-api.alpaca.markets/v2/orders` for paper trading. The request surface includes `symbol`, `qty`, `side`, `type`, `time_in_force`, `limit_price`, and `client_order_id`, which line up well with the repository's existing `BrokerOrderRequest` model. The reference also notes that `403` represents insufficient buying power or shares and `422` represents invalid or unrecognized inputs, which gives a clean first-pass mapping to terminal rejection behavior.[2]

## References

[1]: https://docs.alpaca.markets/docs/authentication
[2]: https://docs.alpaca.markets/reference/postorder
