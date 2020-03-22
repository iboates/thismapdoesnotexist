	with building as (
		select
			osm_id,
			ST_Centroid(way) as geom
		from
			planet_osm_polygon
		where
			building is not null
	),

	pois as (
		select
			p.way as geom,
			count(b.geom) as numbuilding
		from
			planet_osm_point as p,
			building as b
		where
			p.place in ('village', 'town', 'suburb')
			and
			ST_DWithin(b.geom, p.way, 250)
		group by
			p.way
	)

	select
		ST_Envelope(ST_Buffer(geom, 500)) as geom
	from
		pois
	where
		numbuilding >= 50