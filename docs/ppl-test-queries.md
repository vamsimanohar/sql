# PPL Test Queries & Index Setup

Quick-reference for manual testing against a live OpenSearch cluster.
Data files live in `sql/doctest/test_data/*.json`.

---

## Index Setup (Bulk Ingest)

Run these to create and populate all required test indices.

### accounts (4 docs, used by most commands)
```bash
curl -s -XPOST 'localhost:9200/accounts/_bulk?refresh=true' -H 'Content-Type: application/json' --data-binary '
{"index":{"_id":"1"}}
{"account_number":1,"balance":39225,"firstname":"Amber","lastname":"Duke","age":32,"gender":"M","address":"880 Holmes Lane","employer":"Pyrami","email":"amberduke@pyrami.com","city":"Brogan","state":"IL"}
{"index":{"_id":"6"}}
{"account_number":6,"balance":5686,"firstname":"Hattie","lastname":"Bond","age":36,"gender":"M","address":"671 Bristol Street","employer":"Netagy","email":"hattiebond@netagy.com","city":"Dante","state":"TN"}
{"index":{"_id":"13"}}
{"account_number":13,"balance":32838,"firstname":"Nanette","lastname":"Bates","age":28,"gender":"F","address":"789 Madison Street","employer":"Quility","city":"Nogal","state":"VA"}
{"index":{"_id":"18"}}
{"account_number":18,"balance":4180,"firstname":"Dale","lastname":"Adams","age":33,"gender":"M","address":"467 Hutchinson Court","employer":null,"email":"daleadams@boink.com","city":"Orick","state":"MD"}
'
```

### state_country (8 docs, used by join/explain/streamstats)
```bash
curl -s -XPOST 'localhost:9200/state_country/_bulk?refresh=true' -H 'Content-Type: application/json' --data-binary '
{"index":{"_id":"1"}}
{"name":"Jake","age":70,"state":"California","country":"USA","year":2023,"month":4}
{"index":{"_id":"2"}}
{"name":"Hello","age":30,"state":"New York","country":"USA","year":2023,"month":4}
{"index":{"_id":"3"}}
{"name":"John","age":25,"state":"Ontario","country":"Canada","year":2023,"month":4}
{"index":{"_id":"4"}}
{"name":"Jane","age":20,"state":"Quebec","country":"Canada","year":2023,"month":4}
{"index":{"_id":"5"}}
{"name":"Jim","age":27,"state":"B.C","country":"Canada","year":2023,"month":4}
{"index":{"_id":"6"}}
{"name":"Peter","age":57,"state":"B.C","country":"Canada","year":2023,"month":4}
{"index":{"_id":"7"}}
{"name":"Rick","age":70,"state":"B.C","country":"Canada","year":2023,"month":4}
{"index":{"_id":"8"}}
{"name":"David","age":40,"state":"Washington","country":"USA","year":2023,"month":4}
'
```

### occupation (6 docs, used by join examples)
```bash
curl -s -XPOST 'localhost:9200/occupation/_bulk?refresh=true' -H 'Content-Type: application/json' --data-binary '
{"index":{"_id":"1"}}
{"name":"Jake","occupation":"Engineer","country":"England","salary":100000,"year":2023,"month":4}
{"index":{"_id":"2"}}
{"name":"Hello","occupation":"Artist","country":"USA","salary":70000,"year":2023,"month":4}
{"index":{"_id":"3"}}
{"name":"John","occupation":"Doctor","country":"Canada","salary":120000,"year":2023,"month":4}
{"index":{"_id":"4"}}
{"name":"David","occupation":"Doctor","country":"USA","salary":120000,"year":2023,"month":4}
{"index":{"_id":"5"}}
{"name":"David","occupation":"Unemployed","country":"Canada","salary":0,"year":2023,"month":4}
{"index":{"_id":"6"}}
{"name":"Jane","occupation":"Scientist","country":"Canada","salary":90000,"year":2023,"month":4}
'
```

### employees (used by basic queries)
```bash
curl -s -XPOST 'localhost:9200/employees/_bulk?refresh=true' -H 'Content-Type: application/json' --data-binary '
{"index":{"_id":"1"}}
{"name":"Alice","age":30,"department":"Engineering","salary":90000}
{"index":{"_id":"2"}}
{"name":"Bob","age":35,"department":"Marketing","salary":75000}
{"index":{"_id":"3"}}
{"name":"Carol","age":28,"department":"Engineering","salary":85000}
{"index":{"_id":"4"}}
{"name":"Dave","age":42,"department":"Sales","salary":70000}
{"index":{"_id":"5"}}
{"name":"Eve","age":31,"department":"Engineering","salary":95000}
{"index":{"_id":"6"}}
{"name":"Frank","age":45,"department":"Marketing","salary":80000}
{"index":{"_id":"7"}}
{"name":"Grace","age":27,"department":"Sales","salary":65000}
{"index":{"_id":"8"}}
{"name":"Hank","age":38,"department":"Engineering","salary":105000}
'
```

### people (used by functions: math, string, datetime, crypto, collection, conversion)
```bash
curl -s -XPOST 'localhost:9200/people/_bulk?refresh=true' -H 'Content-Type: application/json' --data-binary '
{"index":{"_id":"1"}}
{"name":"Alice","age":30,"city":"Seattle"}
{"index":{"_id":"2"}}
{"name":"Bob","age":25,"city":"Portland"}
{"index":{"_id":"3"}}
{"name":"Carol","age":35,"city":"Vancouver"}
'
```

### products (used by basic queries)
```bash
curl -s -XPOST 'localhost:9200/products/_bulk?refresh=true' -H 'Content-Type: application/json' --data-binary '
{"index":{"_id":"1"}}
{"name":"Widget","price":9.99,"category":"Tools","stock":100}
{"index":{"_id":"2"}}
{"name":"Gadget","price":24.99,"category":"Electronics","stock":50}
{"index":{"_id":"3"}}
{"name":"Doohickey","price":4.99,"category":"Tools","stock":200}
{"index":{"_id":"4"}}
{"name":"Thingamajig","price":49.99,"category":"Electronics","stock":25}
{"index":{"_id":"5"}}
{"name":"Whatchamacallit","price":14.99,"category":"Misc","stock":75}
{"index":{"_id":"6"}}
{"name":"Gizmo","price":34.99,"category":"Electronics","stock":30}
'
```

### Ingest ALL at once
```bash
# One-liner to ingest all indices (copy-paste friendly)
for idx in accounts state_country occupation employees people products; do
  echo "--- $idx ---"
done
# Or run each curl block above individually
```

### Enable distributed execution
```bash
curl -s -XPUT 'localhost:9200/_cluster/settings' -H 'Content-Type: application/json' -d '{
  "persistent": {"plugins.ppl.distributed.enabled": true}
}'
```

### Disable distributed execution (revert to legacy)
```bash
curl -s -XPUT 'localhost:9200/_cluster/settings' -H 'Content-Type: application/json' -d '{
  "persistent": {"plugins.ppl.distributed.enabled": false}
}'
```

---

## PPL Queries by Category

Helper function for running queries:
```bash
ppl() { curl -s 'localhost:9200/_plugins/_ppl' -H 'Content-Type: application/json' -d "{\"query\":\"$1\"}" | python3 -m json.tool; }
```

---

### Join Queries (state_country + occupation)

```bash
# Inner join
ppl "source = state_country | inner join left=a right=b ON a.name = b.name occupation | fields a.name, a.age, b.occupation, b.salary"

# Left join
ppl "source = state_country as a | left join left=a right=b ON a.name = b.name occupation as b | fields a.name, a.age, b.occupation, b.salary"

# Right join (requires plugins.calcite.all_join_types.allowed=true)
ppl "source = state_country as a | right join left=a right=b ON a.name = b.name occupation as b | fields a.name, a.age, b.occupation, b.salary"

# Semi join
ppl "source = state_country as a | left semi join left=a right=b ON a.name = b.name occupation as b | fields a.name, a.age, a.country"

# Anti join
ppl "source = state_country as a | left anti join left=a right=b ON a.name = b.name occupation as b | fields a.name, a.age, a.country"

# Join with filter
ppl "source = state_country | inner join left=a right=b ON a.name = b.name occupation | where b.salary > 80000 | fields a.name, b.salary"

# Join with sort + limit
ppl "source = state_country | inner join left=a right=b ON a.name = b.name occupation | sort - b.salary | head 3"

# Join with subsearch
ppl "source = state_country as a | left join ON a.name = b.name [ source = occupation | where salary > 0 | fields name, country, salary | sort salary | head 3 ] as b | fields a.name, a.age, b.salary"

# Join with stats
ppl "source = state_country | inner join left=a right=b ON a.name = b.name occupation | stats avg(salary) by span(age, 10) as age_span, b.country"
```

### Explain (shows distributed plan)
```bash
# Explain a join query
curl -s 'localhost:9200/_plugins/_ppl/_explain' -H 'Content-Type: application/json' \
  -d '{"query":"source = state_country | inner join left=a right=b ON a.name = b.name occupation | fields a.name, b.salary"}' | python3 -m json.tool

# Explain a simple query
curl -s 'localhost:9200/_plugins/_ppl/_explain' -H 'Content-Type: application/json' \
  -d '{"query":"source = accounts | where age > 30 | head 5"}' | python3 -m json.tool
```

---

### Basic Scan / Filter / Limit (accounts)

```bash
ppl "source=accounts"
ppl "source=accounts | head 2"
ppl "source=accounts | fields firstname, age"
ppl "source=accounts | where age > 30"
ppl "source=accounts | where age > 30 | fields firstname, age"
ppl "source=accounts | where age > 30 | head 2"
ppl "source=accounts | fields firstname, age | head 3 from 1"
```

### Sort (accounts)
```bash
ppl "source=accounts | sort age | fields firstname, age"
ppl "source=accounts | sort - balance | fields firstname, balance | head 3"
ppl "source=accounts | sort + age | fields firstname, age"
```

### Rename (accounts)
```bash
ppl "source=accounts | rename firstname as first_name | fields first_name, age"
ppl "source=accounts | rename firstname as first_name, lastname as last_name | fields first_name, last_name"
```

### Where / Filter (accounts)
```bash
ppl "source=accounts | where gender = 'M' | fields firstname, gender"
ppl "source=accounts | where age > 30 AND gender = 'M' | fields firstname, age, gender"
ppl "source=accounts | where balance > 10000 | fields firstname, balance"
ppl "source=accounts | where employer IS NOT NULL | fields firstname, employer"
```

### Dedup (accounts)
```bash
ppl "source=accounts | dedup gender | fields account_number, gender | sort account_number"
ppl "source=accounts | dedup 2 gender | fields account_number, gender | sort account_number"
```

### Eval (accounts)
```bash
ppl "source=accounts | eval doubleAge = age * 2 | fields age, doubleAge"
ppl "source=accounts | eval greeting = 'Hello ' + firstname | fields firstname, greeting"
```

### Stats / Aggregation (accounts)
```bash
ppl "source=accounts | stats count()"
ppl "source=accounts | stats avg(age)"
ppl "source=accounts | stats avg(age) by gender"
ppl "source=accounts | stats max(age), min(age) by gender"
ppl "source=accounts | stats count() as cnt by state"
```

### Parse (accounts)
```bash
ppl "source=accounts | parse email '.+@(?<host>.+)' | fields email, host"
ppl "source=accounts | parse address '\\d+ (?<street>.+)' | fields address, street"
```

### Regex (accounts)
```bash
ppl "source=accounts | regex email=\"@pyrami\\.com$\" | fields account_number, email"
```

### Fillnull (accounts)
```bash
ppl "source=accounts | fields email, employer | fillnull with '<not found>' in employer"
```

### Replace (accounts)
```bash
ppl "source=accounts | replace \"IL\" WITH \"Illinois\" IN state | fields state"
```

---

### Streamstats (state_country)
```bash
ppl "source=state_country | streamstats avg(age) as running_avg, count() as running_count by country"
ppl "source=state_country | streamstats current=false window=2 max(age) as prev_max_age"
```

### Explain (state_country)
```bash
ppl "explain source=state_country | where country = 'USA' OR country = 'England' | stats count() by country"
```

---

### Functions (people)
```bash
ppl "source=people | eval len = LENGTH(name) | fields name, len"
ppl "source=people | eval upper = UPPER(name) | fields name, upper"
ppl "source=people | eval abs_val = ABS(-42) | fields name, abs_val | head 1"
```

---

## Index Summary

| Index | Docs | Used By | Key Fields |
|-------|------|---------|------------|
| `accounts` | 4 | head, stats, where, sort, dedup, eval, parse, regex, fillnull, rename, replace, fields, addtotals, transpose, appendpipe, condition, expressions, statistical, aggregations, relevance | account_number, balance, firstname, lastname, age, gender, address, employer, email, city, state |
| `state_country` | 8 | join, explain, streamstats | name, age, state, country, year, month |
| `occupation` | 6 | join | name, occupation, country, salary, year, month |
| `employees` | 8 | basic queries | name, age, department, salary |
| `people` | 3 | math, string, datetime, crypto, collection, conversion functions | name, age, city |
| `products` | 6 | basic queries | name, price, category, stock |

### Additional indices in doctest/test_data/ (ingest from file if needed)
| Index | Data File |
|-------|-----------|
| `books` | `doctest/test_data/books.json` |
| `nyc_taxi` | `doctest/test_data/nyc_taxi.json` |
| `weblogs` | `doctest/test_data/weblogs.json` |
| `json_test` | `doctest/test_data/json_test.json` |
| `otellogs` | `doctest/test_data/otellogs.json` |
| `mvcombine_data` | `doctest/test_data/mvcombine.json` |
| `work_information` | `doctest/test_data/work_information.json` |
| `worker` | `doctest/test_data/worker.json` |
| `events` | `doctest/test_data/events.json` |

To ingest from file:
```bash
curl -s -XPOST 'localhost:9200/<index_name>/_bulk?refresh=true' \
  -H 'Content-Type: application/json' \
  --data-binary @sql/doctest/test_data/<file>.json
```
