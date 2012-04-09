-- track_at_time(geometry,geometry start time, time to find)
-- Takes a Linestring Geometry with measure values representing offset in seconds from a given time, the start time, and the time to extract a point for
CREATE OR REPLACE FUNCTION track_at_time(geom geometry, trackstime timestamp with time zone, findtime timestamp with time zone)
RETURNS geometry AS
$BODY$
WITH t AS (
	SELECT 
		$1 AS track,
		$2 AS trackstart,
		(ST_M(ST_EndPoint($1))::text || ' seconds')::interval + $2 AS trackend,
		$3 AS findtime
	)
SELECT
	CASE
		WHEN findtime NOT BETWEEN trackstart and trackend
			THEN NULL
		ELSE
			ST_Locate_Along_Measure(
				track,
				trackstart + EXTRACT(epoch FROM findtime-trackstart)
			)
	END
;	
$BODY$
LANGUAGE sql;

-- track_at_time(geometry,geometry start time, range start, range end)
-- Takes a Linestring Geometry with measure values representing offset in seconds from a given time, the start time, and the time range to extract 
CREATE OR REPLACE FUNCTION track_at_time(geom geometry, trackstime timestamp with time zone, periodstime timestamp with time zone, periodetime timestamp with time zone)
RETURNS geometry AS
$BODY$
WITH t AS (
	SELECT 
		$1 AS track,
		$2 AS trackstart,
		(ST_M(ST_EndPoint($1))::text || ' seconds')::interval + $2 AS trackend,
		$3 AS periodstart,
		$4 AS periodend
	)
SELECT
	CASE
		WHEN trackstart >= periodend OR trackend <= periodstart
			THEN NULL
		WHEN  trackstart >= periodstart AND trackend <= periodend
			THEN track
		ELSE
			ST_Locate_Between_Measures(
				track,
				trackstart + GREATEST(0,EXTRACT(epoch FROM periodstart-trackstart)),
				trackstart + GREATEST(0,EXTRACT(epoch FROM periodend-trackstart))
			)
	END
;	
$BODY$
LANGUAGE sql;

--track_kmltrack(4d linestring,start time of track)
CREATE OR REPLACE FUNCTION track_kmltrack(geometry, timestamp with time zone)
RETURNS text AS
$BODY$select 
E'<gx:Track>
    <altitudeMode>absolute</altitudeMode>
    <extrude>1</extrude>
    ' ||
array_to_string(array_agg(whens),E'\n    ')  || '\n    ' ||
array_to_string(array_agg(coords),E'\n    ') || '\n    ' ||
E'
</gx:Track>' as kml
from
(select 
'<when>' || replace(($2 + ((round(st_m(geom)))::text || ' seconds')::interval)::text,' ','T') || '</when>' as whens,
'<gx:coord>' || st_x(transform(geom,4326))::text || ' ' || st_y(transform(geom,4326))::text || ' ' || round(st_z(geom)+256)::text || '</gx:coord>' as coords
from st_dumppoints($1) as geom) as foo $BODY$
LANGUAGE sql;

-- track_closestpoint(linestring, point) returns the closest point with all dimensions on a linestring to a point
CREATE OR REPLACE FUNCTION track_closestpoint(geometry,geometry)
RETURNS geometry AS
$BODY$
SELECT st_line_interpolate_point($1,st_line_locate_point($1,$2));
$BODY$
LANGUAGE sql;


-- track_closestpoint3d(linestring, point) returns the closest point with all dimensions on a linestring to a point
CREATE OR REPLACE FUNCTION track_closestpoint3d(geometry,geometry)
RETURNS geometry AS
$BODY$
SELECT st_line_interpolate_point($1,st_line_locate_point($1,st_3dclosestpoint($1,$2)));
$BODY$
LANGUAGE sql;


-- track_rms(line 1, start time line 1, line 2, start time line 2, sample interval in seconds)
-- calculates an RMS Error between two tracks where they have time in common
CREATE OR REPLACE FUNCTION rms(ageom geometry, astime timestamp with time zone, bgeom geometry, bstime timestamp with time zone, m integer DEFAULT 10)
  RETURNS double precision AS
$BODY$
declare
stime timestamptz;
etime timestamptz;
aoffset double precision;
boffset double precision;
overlap integer;
count integer;
sumofsquares double precision;
d double precision;
begin
stime:=greatest(astime,bstime);
etime:=least(
	astime + (st_m(st_endpoint(ageom))::text || ' seconds')::interval,
	bstime + (st_m(st_endpoint(bgeom))::text || ' seconds')::interval
);
aoffset:=extract(epoch from stime-astime);
boffset:=extract(epoch from stime-bstime);
overlap:=floor(extract(epoch from etime-stime));
count:=0;
sumofsquares:=0;
FOR i IN 0..overlap BY m LOOP
	count:=count+1;
	d:=st_distance(
		st_locate_along_measure(ageom,i+aoffset),
		st_locate_along_measure(bgeom,i+boffset)
	);
	--raise notice 'd:%',d;
	sumofsquares:=sumofsquares+d^2;
END LOOP;
return sqrt(sumofsquares/count::double precision);

end;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  
  
--track_dumppoints(geometry)
--dumps points out using text processing on ewkt format for linestrings
--much faster than current implementation of st_dumppoints
CREATE OR REPLACE FUNCTION track_dumppoints(geometry)
  RETURNS SETOF geometry AS
$BODY$SELECT
	(
	'SRID=' || 
	bar ||
	';POINT(' || 
	foo || 
	')'
	)::geometry 
FROM 
	regexp_split_to_table(
		rtrim(
			substring(asewkt($1) from 23)
			,')'
		),
		','
	) as foo , srid($1) as bar
WHERE st_geometrytype($1)='ST_LineString';$BODY$
  LANGUAGE sql IMMUTABLE STRICT
  COST 1
  ROWS 100;
  
-- track_loess_r workhorse for track_loess uses plr loess function to smooth a geometry, must be 4d
CREATE OR REPLACE FUNCTION track_loess_r(double precision[], double precision[], double precision[], double precision[], double precision)
  RETURNS SETOF xyzm AS
$BODY$
#pull in array and separate into dimensions
inlist <- arg1
startarray <- array(inlist,dim=c(4,length(inlist)/4))
inx <- arg1
iny <- arg2
inz <- arg3
inm <- arg4
inm = inm - min(inm)
length = length(inm)
maxm <- floor(max(inm))
outm <- array(0:maxm)
#outm <- inm

# set the span; width is in m units convert to a percentage
width <- arg5
minwidth <- 2 * maxm / length
if (width < minwidth){
 width <- minwidth
}
s <- width/maxm
if (s>1){
 s <- 1
} 

outx <- round(predict(loess(inx ~ inm,span=s, degree=2, family='gaussian',control=loess.control(iterations=5)),data.frame(inm=outm)))
outy <- round(predict(loess(iny ~ inm,span=s, degree=2, family='gaussian',control=loess.control(iterations=5)),data.frame(inm=outm)))
outz <- round(predict(loess(inz ~ inm,span=s, degree=2, family='gaussian',control=loess.control(iterations=5)),data.frame(inm=outm))) 

dataframe <- data.frame(x=outx,y=outy,z=outz,m=outm)
return(dataframe) 

$BODY$
  LANGUAGE plr VOLATILE STRICT
  COST 100
  ROWS 1000;

--track_loess(4d linestring, span in seconds)
--formats geometry for call to plr function track_loess_r
CREATE OR REPLACE FUNCTION track_loess(geometry, double precision)
  RETURNS geometry AS
$BODY$DECLARE
rec record;
out geometry;
BEGIN
WITH points AS (
SELECT 
	avg(st_x(d)) as x,
	avg(st_y(d)) as y,
	avg(st_z(d)) as z,
	st_m(d)-min(st_m(d)) OVER () as m
FROM track_dumppoints(st_simplify($1,1)) d
GROUP BY st_m(d) ORDER BY st_m(d)
) SELECT INTO rec array_accum(x) as x,array_accum(y) as y, array_accum(z) as z, array_accum(m) as m FROM points;

SELECT INTO out st_setsrid(st_makeline(st_makepoint(x,y,z,m)),srid($1)) from track_loess_r(rec.x,rec.y,rec.z,rec.m,$2);
return out;
EXCEPTION
WHEN data_exception THEN
raise notice 'error %', rec.m;
WITH points AS (
SELECT 
	avg(st_x(d)) as x,
	avg(st_y(d)) as y,
	avg(st_z(d)) as z,
	st_m(d)-min(st_m(d)) OVER () as m
FROM track_dumppoints(st_simplify($1,1)) d
GROUP BY st_m(d) ORDER BY st_m(d)
)
SELECT INTO out st_setsrid(st_makeline(st_makepoint(x,y,z,m)),srid($1)) FROM points;
return out;
END;
 $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
