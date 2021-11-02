module JDFManager

using DataFrameTools
using SHA
using Dates

export cached_df_fn, from_hash, to_hash, to_metaname, from_metaname, set_jdf_dir

jdf_dir = tempdir()

function set_jdf_dir(d)
	global jdf_dir
	jdf_dir = d
end

cache_dir(dir, hash) = joinpath(jdf_dir, dir, hash * ".jdf")

function cached_df_fn(metaname, fn; dir="JDF", metadata="", useCache=true) 
	if useCache
		df = from_metaname(metaname, dir=dir)
		if df != nothing		
			return df
		end
	end
	df = fn()
	if df == nothing
		return df
	end	
	to_metaname(df, metaname, dir=dir, metadata=metadata)
	df
end

from_metaname(metaname; dir="JDF") = from_hash(bytes2hex(sha256(metaname)), dir=dir, metaname=metaname)

function from_hash(hash; dir="JDF", metaname="")
	jdf = cache_dir(dir, hash)
	if isdir(jdf)
		println(stderr, "Cached ", metaname, " - ", joinpath(dir, hash * ".jdf"))
		return DataFrameTools.df_read(jdf)
	end
end

to_metaname(df, metaname; dir="JDF", metadata="") = to_hash(df, bytes2hex(sha256(metaname)), dir=dir, metafilename=metaname)

function to_hash(df, hash; dir="JDF", metafilename="", metadata="")
	if df == nothing
		return 
	end
	jdf = cache_dir(dir, hash)
	println(stderr, "Caching ", metafilename, " - ", joinpath(dir, hash * ".jdf"))
	println(stderr, "DateTime -> Date")
	DataFrameTools.df_write(jdf, roundtime(df))
	if metafilename != ""
		open(joinpath(jdf, replace(metafilename, ":"=>".") ), "w+") do fid
			write(fid, metadata)
		end
	end
end

function roundtime(df)
	for column in names(df)
		if typeof(df[!, column]) == Vector{Union{Missing, Dates.DateTime}}
			df[!, column] = map(dt-> ismissing(dt) ? missing : convert(Date, dt), df[!, column])
		end
	end
	df
end

######
end