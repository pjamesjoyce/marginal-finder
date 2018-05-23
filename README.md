# marginal-finder
Python implementation of method to find marginal suppliers suggested by Buyle et al (2017)

## Usage

This is currently under development! It only does the first two steps so far, and only for cement.

It uses the UN Comtrade API to get trade data. Production data (for cement - from the USGS minerals yearbook) is in a csv file (in the assets folder).

To try it out you can do this though:

```python

from marginal_finder import ComtradeMarketFinder

belgium = ComtradeMarketFinder(market='Belgium')

belgium.multi_year_markets()

```

This uses the default parameters (tMarket = 5%, tYear = 50%, years = 2003 - 2013 inclusive) and finds the potential geographic extent of the market.

Next steps - to use retrospective trends to identify suppliers with ability to increase production to identify marginal suppliers - to follow.

### Reference
Matthias Buyle, Massimo Pizzol & Amaryllis Audenaert (2017) Identifying marginal suppliers of construction materials: consistent modeling and sensitivity analysis on a Belgian case. Int J Life Cycle Assess DOI 10.1007/s11367-017-1389-5
