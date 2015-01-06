First-class feature generation
==============================

Specification for adding first-class support for feature generation into
Ivory.

Windowing functions
-------------------
Functions applied to sets of values. These functions basically return functions
of the form:

```
type FeatureFunction = (DateTime, Stream[(Value, DateTime)]) => Value
```

* `ref`: First argument is the reference date/time of the snapshot/chord;
* `facts`: Second argument is an ordered stream of timestamped facts (timestamps will
less than or equal to `ref`);
* Returns the computed feature value.

### `latest`
Return the most recent fact in `facts` with respect to `ref`.

### `latestN`
Return the most recent "n" facts if `facts` with respect to `ref`.
Not available for "list" facts. Struct facts can optionally have an additional
`field` specified.

### `count`
Return the number of facts in `facts`.

### `sum`
Return the sum of the facts in `facts`. Only applicable to fact values that can
be summed.

### `count_unique`
Return the number of unique fact values in `facts`.

### `days_since_latest`, `weeks_since_latest`, ...
Return the number of days (or weeks, etc) between the most recent fact in
`facts` and `ref`.

### `days_since_earliest`, `weeks_since_earliest`, ...
Return the number of days (or weeks, etc) between the earliest fact in `facts` and
`ref`.

### `count_by(field)`
Return the number of facts in `facts` grouped by `field`.

### `sum_by(field, sum_field)`
Return the sum of `sum_field` facts in `facts` grouped by `field`.

### `days_since_latest_by(field)`
Return the number of days between the most recent fact in `facts` and `ref`, grouped
by `field`.

### `days_since_earliest_by(field)`
Return the number of days between the earliest fact in `facts` and `ref`, grouped
by `field`.

### `count_days`
Return the number of days that facts in `facts` are attributed to.

### `mean_in_days`, `mean_in_weeks`
Return the number of facts in `facts` attributed to a given day on average.

### `maximum_in_days`, `maximum_in_weeks`
Return the number of facts in `facts` attributed to a given day that is the maximum.

### `minimum_in_days`, `minimum_in_weeks`
Return the number of facts in `facts` attributed to a given day that is the minimum.

### `quantile_in_days(k, q)`, `quantile_in_weeks(k, q)`
Return the number of facts in `facts` attribute to a given day that is the k-th q-quantile (using
an approximate algorithm)

### `proportion_by_time(start_hr, end_hr)`
Return the proportion of facts in `facts` that occur between the hours of the day `start_hr`
and `end_hr`. That is, a count of the facts occuring between those hours divided by the total
number of facts. (Timezone?)

### `proportion_weekdays`
Return the proportion of facts in `facts` that occur on weekdays. That is, a count of the facts
occuring on weekends divided by the total number of facts. (Timezone?)

### `proportion_weekends`
Return the proportion of facts in `facts` that occur on weekends. That is, a count of the facts
occuring on weekends divided by the total number of facts. (Timezone?)

### `proportion(field, value)`
Proportion of facts in `facts` with a value of `value` for field `field`.

### `inverse`
Written before another expression which returns a "continous" ("numerical") result. Takes the
inverse before returning the expression. Note that this functions inverse is not the same as the
JVM implementation under devision by 0 (which is infinity or -infinity for -0), but rather returns
not a number.

### gradient

Only applicable to "continous" ("numerical") facts:

```
// period - number of days, months going back, e.g. 1,2,3,6,12,18 (configurable)
// need to consider calendar months (not just a nubmer of days)
gradient(period):
  v0 = latest value at ref
  vp = latest value at ref - period
  (v0 - vp)/v0  iff v0 != 0, no feature generated otherwise
```

### difference

For "continuous" ("numerical") features:

```
// period - number of days, months going back, e.g. 1,2,3,6,12,18 (configurable)
// need to consider calendar months (not just a nubmer of days)
difference(period):
  v0 = latest value at ref
  vp = latest value at ref - period
  v0 - vp
```

For "categorical" features:

```
// ref - reference time (of snapshot/chord)
// period - number of days, months going back, e.g. 1,2,3,6,12,18 (configurable)
// need to consider calendar months (not just a nubmer of days)
difference(ref, period):
  v0 = latest value at ref
  vp = latest value at ref - period
  if (vp != v0)
    true
  else
    false // OR no feature
```


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
