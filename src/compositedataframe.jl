using Compat

export AbstractCompositeDataFrame, CompositeDataFrame

"""
    AbstractCompositeDataFrame
    
An abstract type that is an AbstractDataFrame. Each type that inherits from
this is expected to be a type-stable data frame. 
"""
abstract AbstractCompositeDataFrame <: AbstractDataFrame

"""
```julia
CompositeDataFrame(columns::Vector{Any}, cnames::Vector{Symbol})
CompositeDataFrame(columns::Vector{Any}, cnames::Vector{Symbol}, typename::Symbol)
CompositeDataFrame(; kwargs...)
CompositeDataFrame(typename::Symbol; kwargs...)
```

A constructor of AbstractCompositeDataFrames that mimics the `DataFrame` 
constructor. 

This uses `eval` to create a new type within the current module. 

### Arguments

* `columns` : contains the contents of the columns
* `cnames` : the names of the columns
* `typename` : the optional name of the type created
* `kwargs` : the key gives the column names, and the value is the column contents

### Returns

A composite type (not immutable) that is an `AbstractCompositeDataFrame`.

### Examples

```julia
df = CompositeDataFrame(Any[1:3, [2, 1, 2]], [:x, :y])
df = CompositeDataFrame(x = 1:3, y = [2, 1, 2])
df = CompositeDataFrame(:MyDF, x = 1:3, y = [2, 1, 2])
```
"""
function CompositeDataFrame(columns::Vector{Any},
                            cnames::Vector{Symbol} = gennames(length(columns)),
                            typename::Symbol = symbol("CompositeDF" * string(gensym())))
    # TODO: length checks
    e = :(type $(typename) <: AbstractCompositeDataFrame end)
    e.args[3].args = Any[:($(cnames[i]) :: $(typeof(columns[i]))) for i in 1:length(columns)]
    eval(current_module(), e)   # create the type
    typ = eval(current_module(), typename)
    return typ(columns...)
end

CompositeDataFrame(; kwargs...) =
    CompositeDataFrame(Any[ v for (k, v) in kwargs ],
                       Symbol[ k for (k, v) in kwargs ])
CompositeDataFrame(typename::Symbol; kwargs...) =
    CompositeDataFrame(Any[ v for (k, v) in kwargs ],
                       Symbol[ k for (k, v) in kwargs ],
                       typename)

# CompositeDataFrame(df::DataFrame) = CompositeDataFrame(df.columns, names(df))

CompositeDataFrame(adf::AbstractDataFrame) =
    CompositeDataFrame(DataFrames.columns(adf), names(adf))
    
CompositeDataFrame(adf::AbstractDataFrame, nms::Vector{Symbol}) =
    CompositeDataFrame(DataFrames.columns(adf), nms)


DataFrames.DataFrame(cdf::AbstractCompositeDataFrame) = DataFrame(DataFrames.columns(cdf), names(cdf))


#########################################
## basic stuff
#########################################

Base.names{T <: AbstractCompositeDataFrame}(cdf::T) = @compat fieldnames(T)

DataFrames.ncol(cdf::AbstractCompositeDataFrame) = length(names(cdf))
DataFrames.nrow(cdf::AbstractCompositeDataFrame) = ncol(cdf) > 0 ? length(cdf.(1))::Int : 0

DataFrames.columns(cdf::AbstractCompositeDataFrame) = Any[ cdf.(i) for i in 1:length(cdf) ]
                
function Base.hcat(df1::AbstractCompositeDataFrame, df2::AbstractCompositeDataFrame)
    nms = DataFrames.make_unique([names(df1); names(df2)])
    columns = Any[DataFrames.columns(df1)..., DataFrames.columns(df2)...]
    return CompositeDataFrame(columns, nms)
end
Base.hcat(df1::DataFrame, df2::AbstractCompositeDataFrame) = hcat(df1, DataFrame(df2))
Base.hcat(df1::AbstractCompositeDataFrame, df2::AbstractDataFrame) = hcat(DataFrame(df1), DataFrame(df2))
Base.hcat(df1::AbstractDataFrame, df2::AbstractCompositeDataFrame) = hcat(DataFrame(df1), DataFrame(df2))

DataFrames.index(cdf::AbstractCompositeDataFrame) = DataFrames.Index(names(cdf))

#########################################
## getindex
#########################################

Base.getindex(cdf::AbstractCompositeDataFrame, col_inds::DataFrames.ColumnIndex) = cdf.(col_inds)
Base.getindex{T <: DataFrames.ColumnIndex}(cdf::AbstractCompositeDataFrame, col_inds::AbstractVector{T}) = CompositeDataFrame(Any[ cdf.(col_inds[i]) for i = 1:length(col_inds) ], names(cdf)[col_inds])
Base.getindex(cdf::AbstractCompositeDataFrame, row_inds, col_inds::DataFrames.ColumnIndex) = cdf.(col_inds)[row_inds]
Base.getindex(cdf::AbstractCompositeDataFrame, row_inds, col_inds) = 
    CompositeDataFrame(Any[ cdf.(col_inds[i])[row_inds] for i = 1:length(col_inds) ],
                       Symbol[ names(cdf)[i] for i = 1:length(col_inds) ])
Base.getindex(cdf::AbstractCompositeDataFrame, row_inds, ::Colon) = typeof(cdf)([cdf.(i)[row_inds] for i in 1:length(cdf)]...)

function Base.getindex(cdf::AbstractCompositeDataFrame, row_inds, col_inds::UnitRange)
    if col_inds.start == 1 && col_inds.stop == length(cdf)
        return typeof(cdf)([ cdf.(i)[row_inds] for i in 1:length(cdf) ]...)
    else
        return CompositeDataFrame(Any[ cdf.(col_inds[i])[row_inds] for i = 1:length(col_inds) ], names(cdf)[col_inds])
    end
end

#########################################
## LINQ-like operations
#########################################


order(d::AbstractCompositeDataFrame; args...) =
    d[sortperm(DataFrame(args...)), :]
                       
transform(d::AbstractCompositeDataFrame; kwargs...) =
    CompositeDataFrame(Any[DataFrames.columns(d)..., [ isa(v, Function) ? v(d) : v for (k,v) in kwargs ]...],
                       Symbol[names(d)..., [ k for (k,v) in kwargs ]...])

Base.select(d::AbstractCompositeDataFrame; kwargs...) =
    CompositeDataFrame(Any[ v for (k,v) in kwargs ],
                       Symbol[ k for (k,v) in kwargs ])
