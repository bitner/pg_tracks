-- Function: askmltrack(geometry, timestamp with time zone)

-- DROP FUNCTION askmltrack(geometry, timestamp with time zone);

CREATE OR REPLACE FUNCTION askmltrack(geometry, timestamp with time zone)
  RETURNS text AS
$BODY$select 
E'<gx:Track>
    <altitudeMode>absolute</altitudeMode>
    <extrude>1</extrude>
    ' ||
array_to_string(array_agg(whens),E'\n    ')  || '\n    ' ||
array_to_string(array_agg(coords),E'\n    ') || '\n    ' ||
array_to_string(array_agg(angles),E'\n    ') || '\n    ' ||
array_to_string(array_agg(speeds),E'\n    ') ||
E'
</gx:Track>' as kml
from
(select 
'<when>' || replace(($2 + ((round(st_m(geom)))::text || ' seconds')::interval)::text,' ','T') || '</when>' as whens,
'<gx:coord>' || st_x(transform(geom,4326))::text || ' ' || st_y(transform(geom,4326))::text || ' ' || round(st_z(geom)+256)::text || '</gx:coord>' as coords,
'<gx:angles>' || 
round(mac_heading(
	lag(geom,2) over (),
	lag(geom,1) over (),
	geom,
	lead(geom,1) over (), 
	lead(geom,2) over ()
)/10.0)*10
|| ' 0 0</gx:angles>' as angles,
E'<speed>' || round(mac_speed( 
	lag(geom,3) over (),
	lag(geom,2) over (),
	lag(geom,1) over (),
	geom,
	lead(geom,1) over (), 
	lead(geom,2) over (),
	lead(geom,3) over ()
	)*2.236936 )::text
|| '</speed>' as speeds
from st_dumppoints($1) as geom) as foo $BODY$
  LANGUAGE sql IMMUTABLE STRICT
  COST 100;
ALTER FUNCTION askmltrack(geometry, timestamp with time zone) OWNER TO pgsql;



CREATE OR REPLACE FUNCTION clean_operation_m(integer)
  RETURNS boolean AS
$BODY$DECLARE
record macnoms.operations;
mintime timestamptz;
maxtime timestamptz;
geom geometry;
m real;
offset real;
delta real;
thism real;
BEGIN
offset:=0;
m:=0;
thism:=0;
SELECT INTO record * from macnoms.operations where opnum=$1;
IF (st_m(st_startpoint(record.the_geom)) !=0 or ((st_m(st_endpoint(record.the_geom))::text || ' seconds')::interval + record.stime) != record.etime) THEN
RAISE NOTICE 'CHECKING %',$1;
BEGIN
	TRUNCATE t;
EXCEPTION WHEN OTHERS THEN
	CREATE TEMP TABLE t (p geometry, x timestamp) ON COMMIT DROP;
END;
m:=ST_M(ST_POINTN(record.the_geom,1));
FOR n in 1..ST_NPOINTS(record.the_geom) LOOP
thism:=ST_M(ST_POINTN(record.the_geom,n));
	IF n>1 THEN
		delta:=thism-ST_M(ST_POINTN(record.the_geom,n-1));
		--raise notice '%,%',m,delta;
		IF  delta < 999 THEN
			m:=m+delta;
		ELSE
			m:=m+round(delta/1000);
			offset:=delta;
		END IF;
	END IF;
	INSERT INTO t VALUES (ST_POINTN(record.the_geom,n),record.stime + (m::text || ' seconds')::interval);
END LOOP;
SELECT INTO mintime min(x) from t;
SELECT INTO maxtime max(x) from t;
UPDATE t SET p=ST_SETSRID(ST_MAKEPOINT(ST_X(p),ST_Y(p),ST_Z(p),EXTRACT(EPOCH FROM x-mintime)),26915);
SELECT INTO geom ST_MakeLine(p) from (SELECT p FROM t order by x) as foo;
--RAISE NOTICE 'wkt: \t%', st_asewkt(geom);
IF NOT (record.the_geom~=geom and record.stime=mintime and record.etime=maxtime) THEN
	RAISE NOTICE 'FIXING %',$1;
	UPDATE macnoms.operations set the_geom=geom,stime=mintime,etime=maxtime where opnum=$1;
END IF;

END IF;
RETURN TRUE;
END;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  
  
  CREATE OR REPLACE FUNCTION clean_operation_timeoffset(integer)
  RETURNS boolean AS
$BODY$DECLARE
record macnoms.operations;
mintime timestamp;
maxtime timestamp;
geom geometry;
BEGIN
SELECT INTO record * from macnoms.operations where opnum=$1;
CREATE TEMP TABLE t (p geometry, x timestamp) ON COMMIT DROP;
FOR n in 1..ST_NPOINTS(record.the_geom) LOOP
	INSERT INTO t VALUES (ST_POINTN(record.the_geom,n),record.stime + (ST_M(ST_POINTN(record.the_geom,n))::text || ' seconds')::interval);
END LOOP;
SELECT INTO mintime min(x) from t;
SELECT INTO maxtime max(x) from t;
UPDATE t SET p=ST_SETSRID(ST_MAKEPOINT(ST_X(p),ST_Y(p),ST_Z(p),EXTRACT(EPOCH FROM x-mintime)),26915);
SELECT INTO geom ST_MakeLine(p) from (SELECT p FROM t order by x) as foo;
RAISE NOTICE 'wkt: %', st_asewkt(geom);
UPDATE macnoms.operations set the_geom=geom,stime=mintime,etime=maxtime where opnum=$1;
DROP TABLE t;
RETURN TRUE;
END;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  
  CREATE OR REPLACE FUNCTION dump_linestring_array(geometry)
  RETURNS double precision[] AS
$BODY$SELECT
regexp_split_to_array(
		rtrim(
			substring(asewkt($1) from 23)
			,')'
		),
		'[, ]'
	)::float8[]
WHERE st_geometrytype($1)='ST_LineString';$BODY$
  LANGUAGE sql IMMUTABLE STRICT
  COST 100;
  
  
  
 CREATE OR REPLACE FUNCTION dump_linestring_points(geometry)
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
  
 CREATE OR REPLACE FUNCTION everynseconds(geometry, integer)
  RETURNS geometry AS
'select st_makeline(st_locate_along_measure($1,g)) from generate_series(0,floor(st_m(st_endpoint($1)))::int,$2) g;'
  LANGUAGE sql VOLATILE
  COST 100;
ALTER FUNCTION everynseconds(geometry, integer) OWNER TO pgsql;

CREATE OR REPLACE FUNCTION iso_to_period(text)
  RETURNS period AS
$BODY$
DECLARE
    debug BOOLEAN := true;
    start_text TEXT;
    end_text TEXT;
    tp_regex TEXT;
    start_time TEXT;
    end_time TIMESTAMPTZ;
    start_time_parts TEXT[];
    end_time_parts TEXT[]; 
    resolution TEXT; 
BEGIN
    start_text := split_part($1,'/',1);
    end_text := split_part($1,'/',2);
    IF debug THEN
        RAISE NOTICE 'Start time: %', start_text;
        RAISE NOTICE 'End time: %', end_text;
    END IF;
    tp_regex := 
        E'(?x)
            ^
            ([[:digit:]]{4})-?
            ([[:digit:]]{2})?
            -?
            ([[:digit:]]{1,2})?
            [T ]?
            ([0-2][[:digit:]])?
            :?
            ([0-5][[:digit:]])?
            :?
            ([0-5][[:digit:]])?
        ';
    start_time_parts := regexp_matches(start_text, tp_regex);
    end_time_parts := regexp_matches(end_text, tp_regex);
    IF debug THEN
        RAISE NOTICE 'Start time parts: %', start_time_parts;
        RAISE NOTICE 'End time parts: %', end_time_parts;
    END IF;
    resolution := CASE 
        WHEN start_time_parts[6] IS NOT NULL THEN 'second'
        WHEN start_time_parts[5] IS NOT NULL THEN 'minute'
        WHEN start_time_parts[4] IS NOT NULL THEN 'hour'
        WHEN start_time_parts[3] IS NOT NULL THEN 'day'
        WHEN start_time_parts[2] IS NOT NULL THEN 'month'
        WHEN start_time_parts[1] IS NOT NULL THEN 'year'
        ELSE NULL
    END;
    IF debug THEN
        RAISE NOTICE 'Resolution: %', resolution;
    END IF;
    IF resolution IS NULL THEN
        RETURN NULL;
    END IF;
    start_time := tp_parts_to_text(start_time_parts);
    IF debug THEN
        RAISE NOTICE 'To Start: % ',start_time;
    END IF;
    end_time := CASE 
        WHEN 
            end_text IS NULL OR
            length(end_text) <= 4
        THEN
            start_time::timestamptz +
            ('1 ' || resolution)::interval
        ELSE 
            date_trunc(
                resolution,
                (tp_parts_to_text(end_time_parts))::timestamptz +
                    ('1 ' || resolution)::interval
            )
        END;
    RETURN period(start_time::timestamptz,end_time);
END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;
  
  CREATE OR REPLACE FUNCTION loess_geom(geometry, double precision)
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
FROM dump_linestring_points(st_simplify($1,1)) d
GROUP BY st_m(d) ORDER BY st_m(d)
) SELECT INTO rec array_accum(x) as x,array_accum(y) as y, array_accum(z) as z, array_accum(m) as m FROM points;

SELECT INTO out st_setsrid(st_makeline(st_makepoint(x,y,z,m)),srid($1)) from loess_array(rec.x,rec.y,rec.z,rec.m,$2);
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
FROM dump_linestring_points(st_simplify($1,1)) d
GROUP BY st_m(d) ORDER BY st_m(d)
)
SELECT INTO out st_setsrid(st_makeline(st_makepoint(x,y,z,m)),srid($1)) FROM points;
return out;
END;
 $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  
  CREATE OR REPLACE FUNCTION loess_array(double precision[], double precision[], double precision[], double precision[], double precision)
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
  
  CREATE OR REPLACE FUNCTION loess_ignoreerrors(geometry, double precision)
  RETURNS geometry AS
$BODY$
DECLARE
out geometry;
BEGIN
return loess($1,$2);
EXCEPTION
WHEN data_exception THEN
raise notice 'error';
return $1;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  
  CREATE OR REPLACE FUNCTION mac_dumppoints_web(IN geometry, IN timestamp with time zone, IN step integer, OUT path integer, OUT x integer, OUT y integer, OUT z integer, OUT "time" timestamp with time zone, OUT heading integer, OUT speed integer)
  RETURNS SETOF record AS
$BODY$
	select * from (select 
		path[1],
		round(st_x(geom))::int,
		round(st_y(geom))::int,
		round(st_z(geom)*3.2808399)::int as z,
		(st_m(geom)::text || ' seconds')::interval + $2 as "time",
		round(mac_heading(lag(geom,2) over (),lag(geom,1) over (),geom,lead(geom,1) over (),lead(geom,2) over ())/10.0)::int * 10 as heading,
		round(mac_speed(lag(geom,3) over (),lag(geom,2) over (),lag(geom,1) over (),geom,lead(geom,1) over (),lead(geom,2) over (),lead(geom,3) over ())*2.236936)::int as speed
	from st_dumppoints(everynseconds($1,$3)) 
	--where 
	--extract(epoch from ((st_m(geom))::text || ' seconds')::interval + $2)::int % $3 = 0 
	) as foo where z>59 and speed >50
 $BODY$
  LANGUAGE sql VOLATILE
  COST 100
  ROWS 1000;
  
  CREATE OR REPLACE FUNCTION speed_at_m(geometry, double precision)
  RETURNS double precision AS
$BODY$
SELECT
	st_length(g)/(st_m(st_endpoint(g))-st_m(st_startpoint(g)))
FROM st_locate_between_measures($1,greatest(0,$2-15),least($2+15,st_m(st_endpoint($1)))) as g

$BODY$
  LANGUAGE sql VOLATILE
  COST 100;
ALTER FUNCTION speed_at_m(geometry, double precision) OWNER TO pgsql;

CREATE OR REPLACE FUNCTION tp_parts_to_text(text[])
  RETURNS text AS
$BODY$
SELECT
        $1[1] || '-' ||
        coalesce($1[2],'01') || '-' ||
        coalesce($1[3],'01') || 'T' ||
        coalesce($1[4],'00') || ':' ||
        coalesce($1 [5],'00') || ':' ||
        coalesce($1[6],'00')
    ;
$BODY$
  LANGUAGE sql IMMUTABLE
  COST 10;
  
  
  CREATE OR REPLACE FUNCTION track_by_time(geom geometry, trackstime timestamp with time zone, periodstime timestamp with time zone, periodetime timestamp with time zone, subset boolean)
  RETURNS geometry AS
$BODY$
SELECT
CASE
	-- Subset is set to false or track entirely within period
	WHEN $5=false OR ($2 >= $3 AND $2 + (st_m(st_endpoint($1))::text || ' seconds')::interval <= $4)
	THEN $1
	-- Period is entirely within track start/end
	WHEN $2 < $3 AND $2 + (st_m(st_endpoint($1))::text || ' seconds')::interval > $4
	THEN st_locate_between_measures(
		$1,
		extract(epoch from $3-$2),
		extract(epoch from $2 + (st_m(st_endpoint($1))::text || ' seconds')::interval - $4)
	)
	WHEN $2 > $3 AND $2 + (st_m(st_endpoint($1))::text || ' seconds')::interval <= $4
	THEN st_locate_between_measures(
		$1,
		0,
		extract(epoch from $2 + (st_m(st_endpoint($1))::text || ' seconds')::interval - $4)
	)
	WHEN $2 <= $3 AND $2 + (st_m(st_endpoint($1))::text || ' seconds')::interval <= $4
	THEN st_locate_between_measures(
		$1,
		extract(epoch from $3-$2),
		999999999999999999
	)
	ELSE NULL
END;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;
  
  
  
CREATE OR REPLACE FUNCTION track_by_time_period(geometry, timestamp with time zone, timestamp with time zone, timestamp with time zone, timestamp with time zone)
  RETURNS geometry AS
$BODY$select 
case when 
period_cc($2,$3) <@ period_cc($4,$5) then $1
 when
period_cc($2,$3) @> period_cc($4,$5) then st_locate_between_measures($1,extract(epoch from period_offset(period_cc($2,$3),$4)),extract(epoch from period_offset(period_cc($2,$3),$5)))
 when 
--period_cc($2,$3) &> period_cc($4,$5) then st_locate_between_measures($1,0,extract(epoch from period_offset(period_cc($4,$5),$2)))
period_cc($2,$3) &> period_cc($4,$5) then st_locate_between_measures($1,0,extract(epoch from ($5-$2)))
 when 
--period_cc($2,$3) &< period_cc($4,$5) then st_locate_between_measures($1,extract(epoch from period_offset(period_cc($4,$5),$3)),9999999)
period_cc($2,$3) &< period_cc($4,$5) then st_locate_between_measures($1,extract(epoch from ($4-$2)),999999999999999999999)
else
null
end;$BODY$
  LANGUAGE sql IMMUTABLE
  COST 100;
  
  
  

  
  
  
  
  
  
  

 
  
  
  
