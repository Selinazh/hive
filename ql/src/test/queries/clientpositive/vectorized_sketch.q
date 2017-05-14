SET hive.vectorized.execution.enabled = true;

explain 
select estimate_sketch(data_to_sketch(cint,16384,1.0))
from alltypesorc;

select estimate_sketch(data_to_sketch(cint,16384,1.0))
from alltypesorc;

explain
select estimate_sketch(data_to_sketch(cfloat,16384,0.5))
from alltypesorc;

select estimate_sketch(data_to_sketch(cfloat,16384,0.5))
from alltypesorc;

explain
select estimate_sketch(data_to_sketch(cstring1,1024,1.0))
from alltypesorc;

select estimate_sketch(data_to_sketch(cstring1,1024,1.0))
from alltypesorc;

SET hive.vectorized.execution.enabled = false;

explain
select estimate_sketch(data_to_sketch(cstring1,1024,1.0))
from alltypesorc;

select estimate_sketch(data_to_sketch(cstring1,1024,1.0))
from alltypesorc;
