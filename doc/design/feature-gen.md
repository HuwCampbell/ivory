First-class feature generation
==============================

Specification for adding first-class support for feature generation into
Ivory.

Feature examples
----------------

### Retail examples

Derive features from a primitive point-of-sale feature type, e.g.:

```
(transaction_id: string, store_id: string, sku_id: string, sales_amnt: int)
```

* Weekly average maximum spend - for each week over a 12 week period (per HH), calculate total
daily sales, take the weekly maximum of those daily sales, then average across the 12 weeks,
ignoring weeks with null or negative sales.

* Lapsing HHs - sum the last 6 weeks sales, and the 6 weeks prior to that, subtract the two and
if the former is less than 60% of the latter, flag as ‘lapsed’.

* Yearly spend - sum the last 52 weeks sales

* Yearly SKUs - count the number of SKUs purchased in the last 52 weeks

* Yearly distinct SKUs - count the number of distinct SKUs purchased in the last 52 weeks

* Yearly carts - count number of distinct transactions in the last 52 weeks

* Average transaction size over the last 12 months

* Average monthly spend over the last 6 months

* Minimum weekly spend in the last 3 months

* Maximum spend in the last 2 weeks

* Per-SKU (product) features:

  * Yearly purchases - number of times purchased in the last 52 weeks

  * Yearly average spend - average amount spent in the last 52 weeks

  * Yearly total spend - total amount spent in the last 52 weeks

  * Yearly inter-purchase interval (IPI) - number of days between the first and last
  purchase divided by the number of purchases in the last 52 weeks

  * Median monthly spend over the last 2 years

  * Maximum spend in the last 6 months


* Purchased baby products at least twice in last 6 weeks
  * the mapping of SKU to the baby product category may not be known up-front ...
  * or could be ... may be Ivory could maintain small mapping tables?

* Purchased meat prdoducts at least twice in last 12 weeks


### Telco examples

Derive features from a primitive call-record feature type, e.g.:

```
(
  tchnlgy_cd:     string,     // e.g. "VC" == voice calls, "SM" == SMS
  inbnd_outbnd:   string,     // e.g. "I" == inbound, "O" == outbound
  call_dur:       int,        // call duration
  cntry_cd:       string      // country code
)
```

* Total calls in last week

* Total SMS in the last 2 weeks

* Duration of inbound calls in last month

* Difference in total outbound calls in the last month vs 2 months before that

* Proportion of international calls to national calls in last 6 weeks

* Gradient of weekly output call duration over last 2 months - compute total outbound
call duration in 7 day windows over the last 7 x 8 = 56 days, then compute the gradient
over those 8 values


### Online media example

Derive features from web-log event records, e.g.:

```
(
  session_id: String,
  article_id: string,
  headline: string,
  content_type: string,
  section_id: String,
  duration: Long
  ...
)
```

* Number of times each article was read where the `headline` is not empty and the `content_type` contains
the string `story`.

* Total number of events in the last 12 months

* Total number of days in the last 3 month that events occurred

* Average number of daily events between 11am - 3pm over the last 2 months

* Average number of weekend events over the last 3 weeks

* Average number of weekday events over the last 5 weeks

* Mean number of events per session over the last 6 weeks

* Average duration of sessions over the last 1 month

* Gradient of weekly average session duration over the last 8 weeks

* Maximum daily events over the last 1 month

* First quantile (0.1) daily event count over the last 2 months

* Total number of events for section "foo" in the last 6 weeks

* Proportion of events for section "foo" compared to all events in the last 6 weeks

* Gradient of weekly proporition of events for section "foo" compared to all weekly events over the last 8 weeks

* Mean days per month of activity over the last 4 months


### Banking examples

* TODO


### General date examples

* Number of days since the last call

* Number of weeks since last purchase


High level steps
----------------

1. Improve dictionary
  * thrift
  * versioning
  * migration tooling
  * update import-dictionary to take thrift structs

2. Add support for complex encodings for facts
  * a way to describe it in the dictionary
    * 'struct' here means a "named tuple", i.e. a product type
    * likely representation would be array of Values
    * also support arrays of values in general
  * dictionary 'encoding' format
  * "fact" representation
  * "import"/"export" representation

3. Add support for windows
  * Add explicit "window" argument in dictionary, e.g. 'latest', 'x-days', 'x-months'
  * Expose "reduce" function for extract, e.g. (e, a, [(v, t)]) => (e, a, v, t)

4. Add support for Meta features
  * Alias "real" features or parts-of

5. Add support for Set features
  * Define operation in dictionary, e.g. avg, min, max, latest, count, sum, histogram
  * Map those to customer "reduce" functions that get called first
  * Define over keys of structs

6. Scalar combination of features
  * This is essentially a "row-level" operation
  * Supporting this also means opening the door to allow scoring to be supported on
  top of Ivory
