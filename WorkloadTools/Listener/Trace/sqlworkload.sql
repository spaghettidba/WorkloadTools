
-- Create a Queue
declare @rc int
declare @TraceID int
declare @maxfilesize bigint
set @maxfilesize = '{0}';

declare @maxnumfiles int
set @maxnumfiles = '{1}';


exec @rc = sp_trace_create @TraceID output, 2, N'{2}', @maxfilesize, NULL, @maxnumfiles
if (@rc != 0) goto error


-- Set the events
declare @on bit
set @on = 1
exec sp_trace_setevent @TraceID, 10,  1, @on
exec sp_trace_setevent @TraceID, 10, 10, @on
exec sp_trace_setevent @TraceID, 10,  8, @on
exec sp_trace_setevent @TraceID, 10, 11, @on
exec sp_trace_setevent @TraceID, 10, 12, @on
exec sp_trace_setevent @TraceID, 10, 13, @on
exec sp_trace_setevent @TraceID, 10, 14, @on
exec sp_trace_setevent @TraceID, 10, 16, @on
exec sp_trace_setevent @TraceID, 10, 17, @on
exec sp_trace_setevent @TraceID, 10, 18, @on
exec sp_trace_setevent @TraceID, 10, 35, @on
exec sp_trace_setevent @TraceID, 10,  3, @on
exec sp_trace_setevent @TraceID, 10, 31, @on
exec sp_trace_setevent @TraceID, 10, 51, @on

exec sp_trace_setevent @TraceID, 12,  1, @on
exec sp_trace_setevent @TraceID, 12, 11, @on
exec sp_trace_setevent @TraceID, 12,  8, @on
exec sp_trace_setevent @TraceID, 12, 10, @on
exec sp_trace_setevent @TraceID, 12, 12, @on
exec sp_trace_setevent @TraceID, 12, 13, @on
exec sp_trace_setevent @TraceID, 12, 14, @on
exec sp_trace_setevent @TraceID, 12, 16, @on
exec sp_trace_setevent @TraceID, 12, 17, @on
exec sp_trace_setevent @TraceID, 12, 18, @on
exec sp_trace_setevent @TraceID, 12, 35, @on
exec sp_trace_setevent @TraceID, 12,  3, @on
exec sp_trace_setevent @TraceID, 12, 31, @on
exec sp_trace_setevent @TraceID, 12, 51, @on

exec sp_trace_setevent @TraceID, 82,  1, @on
exec sp_trace_setevent @TraceID, 82,  2, @on
exec sp_trace_setevent @TraceID, 82, 11, @on
exec sp_trace_setevent @TraceID, 82,  8, @on
exec sp_trace_setevent @TraceID, 82, 10, @on
exec sp_trace_setevent @TraceID, 82, 12, @on
exec sp_trace_setevent @TraceID, 82, 14, @on
exec sp_trace_setevent @TraceID, 82, 35, @on 
exec sp_trace_setevent @TraceID, 82,  3, @on
exec sp_trace_setevent @TraceID, 82, 51, @on

exec sp_trace_setevent @TraceID, 83,  1, @on
exec sp_trace_setevent @TraceID, 83,  2, @on
exec sp_trace_setevent @TraceID, 83, 11, @on
exec sp_trace_setevent @TraceID, 83,  8, @on
exec sp_trace_setevent @TraceID, 83, 10, @on
exec sp_trace_setevent @TraceID, 83, 12, @on
exec sp_trace_setevent @TraceID, 83, 14, @on
exec sp_trace_setevent @TraceID, 83, 35, @on 
exec sp_trace_setevent @TraceID, 83,  3, @on
exec sp_trace_setevent @TraceID, 83, 51, @on


-- Set the Filters
exec sp_trace_setfilter @TraceID, 10 ,  0, 1, N'WorkloadTools'; 
{3}

-- Set the trace status to start
exec sp_trace_setstatus @TraceID, 1

-- display trace id for future references
select TraceID=@TraceID
goto finish

error: 
select ErrorCode=@rc

finish: 

