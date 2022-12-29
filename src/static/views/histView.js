export default class HistView {
    constructor(element, width = 300, height = 300) {
        this.tableViewElement = element;

        this.width = width;
        this.height = height;
    }

    clear() {
        this.tableViewElement
            .selectAll("div")
            .remove()
    }

    addVisDiv() {
        let visDiv = this.tableViewElement
            .append("div")
            .classed("histogram", true)

        visDiv.style("width", this.width + "px")
        visDiv.style("height", this.height + "px")

        return visDiv
    }

    // Draw a histogram using d3.js based on input parameter data
    drawHistogram(jsonData, visDiv = null) {
        let data = Array.from(jsonData.data)
        if (visDiv == null) {
            visDiv = this.addVisDiv()
        }

        // Set the dimensions of the canvas / graph
        var margin = { top: 30, right: 20, bottom: 100, left: 50 },
            width = this.width - margin.left - margin.right,
            height = this.height - margin.top - margin.bottom;

        // Set the ranges
        var x = d3.scaleBand()
            .range([0, width])
            .padding(0.1);
        var y = d3.scaleLinear()
            .range([height, 0]);

        // Define the axes
        var xAxis = d3.axisBottom(x)
        var yAxis = d3.axisLeft(y)
            .ticks(10);

        // Adds the svg canvas
        var svg = visDiv.append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform",
                "translate(" + margin.left + "," + margin.top + ")");

        // Scale the range of the data in the domains
        x.domain(data.map(function (d) { return d[1]; }));
        y.domain([0, d3.max(data, function (d) { return d[0]; })]);

        // append the rectangles for the bar chart
        svg.selectAll(".bar")
            .data(data)
            .enter().append("rect")
            .attr("class", "bar")
            .attr("x", function (d) { return x(d[1]); })
            .attr("width", x.bandwidth())
            .attr("y", function (d) { return y(d[0]); })
            .attr("height", function (d) { return height - y(d[0]); });

        // add the x Axis
        svg.append("g")
            .attr('class', 'x axis')
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis);

        // add the y Axis
        svg.append("g")
            .call(yAxis);

        // rotate x axis labels
        svg.selectAll(".x.axis text")
            .attr("transform", "rotate(-45)")
            .attr("dx", "-.8em")
            .attr("dy", ".25em")
            .style("text-anchor", "end")
            .style("font-size", "10px");


    }

}
