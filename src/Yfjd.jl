module Yfjd

using Dates
using DataFrames
using TimeSeries
using HTTP
using JSON

export get_symbols_df, get_symbols_df_raw
export get_symbols, get_symbols_ta, get_symbols_ta_clo, get_symbols_ta_adj

# deleterows! becomes delete!  since DataFrames v0.20 (breaking)
if  isdefined(DataFrames,:deleterows!)
  Ydelrows! = DataFrames.deleterows!
elseif isdefined(DataFrames,:delete!)
  Ydelrows! = DataFrames.delete!
else
  error("Cant find \"Dataframes.delete[rows]!\" ")
end

"""

# Download raw stock market data from Yahoo Finance
# Returns a DataFrame

    get_symbols_df_raw(symbol, from, to)

## Arguments

    - `symbol`: Market symbol, e.g. "AAPL" or "GOOG"
    - `from` : Date character, e.g. "2018-12-13" in the form of "YYYY-MM-DD"
    - `to` : Date character, e.g. "2019-12-13" in the form of "YYYY-MM-DD"

## Examples

```jldoctest
julia> get_symbols_df_raw("GOOG", "2018-12-26", "2019-12-20")
julia> get_symbols_df_raw("^GDAXI", "2018-12-26", "2019-12-20")
julia> get_symbols_df_raw("EURUSD=X", "2018-12-26", "2019-12-20")
julia> get_symbols_df_raw("BTC-USD", "2018-12-26", "2019-02-20")
```
"""
function get_symbols_df_raw(symbol::String, from::String, to::String)

    from = string(Integer(datetime2unix(DateTime(from * "T12:00:00"))))
    to = string(Integer(datetime2unix(DateTime(to * "T12:00:00"))))
    parse(Int, from) > parse(Int, to) ? error("in get_symbols: to must be older than from") : nothing

    url = "https://query1.finance.yahoo.com/v7/finance/chart/$symbol?&interval=1d&period1=$from&period2=$to"

    response = HTTP.get(url, cookies = true)
    body = JSON.parse(String(response.body))["chart"]["result"][1]
    values = body["indicators"]["quote"][1]

    df = DataFrame(
        Open = values["open"],
        High =  values["high"],
        Low = values["low"],
        Close = values["close"],
        Volume = values["volume"],
        Adjusted = body["indicators"]["adjclose"][1]["adjclose"],
        Time = Dates.Date.(unix2datetime.(body["timestamp"]))
    )

    return df
end

"""

# Download "cooked" stock market data from Yahoo Finance
# Returns a DataFrame
# Cooking is converting to Float64, rounded to 2 decimals
    get_symbols_df(symbol, from, to)

## Arguments

    - `symbol`: Market symbol, e.g. "AAPL" or "GOOG"
    - `from` : Date character, e.g. "2018-12-13" in the form of "YYYY-MM-DD"
    - `to` : Date character, e.g. "2019-12-13" in the form of "YYYY-MM-DD"

## Examples

```jldoctest
julia> get_symbols_df("GOOG", "2018-12-26", "2019-12-20")
julia> get_symbols_df("^GDAXI", "2018-12-26", "2019-12-20")
julia> get_symbols_df("EURUSD=X", "2018-12-26", "2019-12-20")
julia> get_symbols_df("BTC-USD", "2018-12-26", "2019-02-20")
```
"""
function get_symbols_df(symbol::String, from::String, to::String)
    df = get_symbols_df_raw(symbol, from, to)
    Ydelrows!(df, isnothing.(df).Close)
	
	df.Open     = Float64.(df.Open)
	df.High     = Float64.(df.High)
	df.Low      = Float64.(df.Low)
	df.Close    = Float64.(df.Close)
	df.Volume   =   Int64.(df.Volume)
	df.Adjusted = Float64.(df.Adjusted)
	
	df.Open     = round.(df.Open    ,digits=2)
	df.High     = round.(df.High    ,digits=2)
	df.Low      = round.(df.Low     ,digits=2)
	df.Close    = round.(df.Close   ,digits=2)
	df.Adjusted = round.(df.Adjusted,digits=2)
	
    return df
end

"""

# Download stock market data (:Close) from Yahoo Finance
# Returns a TimeArray, so omit times without quotation

    get_symbols_ta_clo(symbol, from, to)

## Arguments

    - `symbol`: Market symbol, e.g. "AAPL" or "GOOG"
    - `from` : Date character, e.g. "2018-12-13" in the form of "YYYY-MM-DD"
    - `to` : Date character, e.g. "2019-12-13" in the form of "YYYY-MM-DD"

## Examples

```jldoctest
julia> get_symbols_ta_clo("GOOG", "2018-12-26", "2019-12-20")
julia> get_symbols_ta_clo("^GDAXI", "2018-12-26", "2019-12-20")
julia> get_symbols_ta_clo("EURUSD=X", "2018-12-26", "2019-12-20")
julia> get_symbols_ta_clo("BTC-USD", "2018-12-26", "2019-02-20")
```
"""
function get_symbols_ta_clo(symbol::String, from::String, to::String)
    df = get_symbols_df(symbol, from, to)
    Ydelrows!(df, isnothing.(df).Close)
    x = TimeArray(df, timestamp = :Time)
    return x
end

"""

# Download stock market data (:Adjusted) from Yahoo Finance
# Returns a TimeArray, so omit times without quotation

    get_symbols_ta_adj(symbol, from, to)

## Arguments

    - `symbol`: Market symbol, e.g. "AAPL" or "GOOG"
    - `from` : Date character, e.g. "2018-12-13" in the form of "YYYY-MM-DD"
    - `to` : Date character, e.g. "2019-12-13" in the form of "YYYY-MM-DD"

## Examples

```jldoctest
julia> get_symbols_ta_adj("GOOG", "2018-12-26", "2019-12-20")
julia> get_symbols_ta_adj("^GDAXI", "2018-12-26", "2019-12-20")
julia> get_symbols_ta_adj("EURUSD=X", "2018-12-26", "2019-12-20")
julia> get_symbols_ta_adj("BTC-USD", "2018-12-26", "2019-02-20")
```
"""
function get_symbols_ta_adj(symbol::String, from::String, to::String)
    df = get_symbols_df(symbol, from, to)
	Ydelrows!(df, isnothing.(df).Adjusted)
    x = TimeArray(df, timestamp = :Time)
    return x
end

"""

# Download stock market data (:Adjusted) from Yahoo Finance
# Returns a TimeArray, so omit times without quotation

    get_symbols_ta(symbol, from, to)

## Arguments

    - `symbol`: Market symbol, e.g. "AAPL" or "GOOG"
    - `from` : Date character, e.g. "2018-12-13" in the form of "YYYY-MM-DD"
    - `to` : Date character, e.g. "2019-12-13" in the form of "YYYY-MM-DD"

## Examples

```jldoctest
julia> get_symbols_ta("GOOG", "2018-12-26", "2019-12-20")
julia> get_symbols_ta("^GDAXI", "2018-12-26", "2019-12-20")
julia> get_symbols_ta("EURUSD=X", "2018-12-26", "2019-12-20")
julia> get_symbols_ta("BTC-USD", "2018-12-26", "2019-02-20")
```
"""
function get_symbols_ta(symbol::String, from::String, to::String)
    return get_symbols_ta_adj(symbol, from, to)
end


"""

# Download stock market data (:Adjusted)from Yahoo Finance
# Returns a TimeArray, so omit times without quotation

    get_symbols(symbol, from, to)

## Arguments

    - `symbol`: Market symbol, e.g. "AAPL" or "GOOG"
    - `from` : Date character, e.g. "2018-12-13" in the form of "YYYY-MM-DD"
    - `to` : Date character, e.g. "2019-12-13" in the form of "YYYY-MM-DD"

## Examples

```jldoctest
julia> get_symbols("GOOG", "2018-12-26", "2019-12-20")
julia> get_symbols("^GDAXI", "2018-12-26", "2019-12-20")
julia> get_symbols("EURUSD=X", "2018-12-26", "2019-12-20")
julia> get_symbols("BTC-USD", "2018-12-26", "2019-02-20")
```
"""
function get_symbols(symbol::String, from::String, to::String)
    return get_symbols_ta_adj(symbol, from, to)
end

end
