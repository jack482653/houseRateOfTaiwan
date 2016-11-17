var features = null;
var jsonpath = "Taiwan.TopoJSON/topojson/counties.json";
(function() {
	var width = 800,
		height = 600;
	var svg = d3.select('svg');
	svg.attr('width', width).attr('height', height);
	var df = d3.format('.2f');
	var tip = d3.tip()
		.attr('class', 'd3-tip')
		.offset([10, 10])
		.html(function(d) {
			return "<strong>" + d.properties.name +":</strong> <span style='color:red'>" + df(d.properties.density / 10000.0) + "萬</span>";
		});
	load = function(type, id) {
		var mappath = 'Taiwan.TopoJSON/topojson/' +
            		(type === 'towns' ? 'towns/towns-' + id : 'counties') + '.json';
		var avgpath = (type === 'towns' ? 'towns' : 'countrys') + 'Avg.json';
		d3.json(mappath, function(error, data) {
			if (error) throw error;

			var features = topojson.feature(
				data, data.objects.map).features,
			bbox = data.bbox,
			scale = Math.min(width / (bbox[2] - bbox[0]),
				height / (bbox[3] - bbox[1])) * 50,
			center = [(bbox[2] + bbox[0]) / 2, (bbox[3] + bbox[1]) / 2];

			d3.json(avgpath, function(error, avg) {
				var max = 0.0;

				for(i = 0; i < features.length; i++) {
					name = features[i].properties.name;
					features[i].properties.density = avg[name];
					max = (max < avg[name])?avg[name]:max;
				}
				var color = d3.scaleQuantize()
					.domain([0.0, max])
					.range(d3.schemeBlues[9]);
				var path = d3.geo.path().projection(
					 d3.geo.mercator()
					 .center(center)
					 .scale(scale)
					 .translate([width / 2, height / 2])
				);
				var svg = d3.select('svg');
				svg.call(tip);
				svg.selectAll("path")
					.data(features).enter()
					.append("path").attr("d",path)
					.attr('fill', function(d) {
						return color(d.properties.density);
					}).on('click', function(d) {
						var id, t;
						if(!d3.event)
							return;
						id = d.properties.id;
						d3.event.stopPropagation();
						if(type === 'towns') {
							t = '';
							id = '';
						} else
							t = 'towns';
						location.search = unescape(encodeURI('type=' + t + '&id=' + id));

					}).on('mouseover', function(d, i) {
						d3.select(this).style(
							'fill-opacity', 0.7);
						tip.show(d);
					}).on('mouseout', function(d, i) {
						d3.selectAll('path').style({
							'fill-opacity':1.0
						});
						tip.hide(d);
					});
	
			});
		});
	}
	parseQueryString = function(queryString) {
		var queries = queryString.split('&'),
		result = {},
		tokens,
		length,
		i;
		for (i = 0, length = queries.length; i < length; i++) {
			tokens = queries[i].split('=');
			result[tokens[0]] = tokens[1];
		}
		return result;
	};

	params = parseQueryString(location.search.substr(1));
	load(params['type'], params['id']);
})();
