export default class VisView {
    constructor(element, width = 1000, height = 600) {
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
            .classed("visualization", true)

        visDiv.style("width", this.width + "px")
        visDiv.style("height", this.height + "px")

        return visDiv
    }

    drawSingleTable(tablename, schema, data, links) {
        this.clear()
        var visDiv = this.addVisDiv()

        // Create a schema row and append it to the table
        let nameRow = visDiv.append("tr");

        // Add a row for table name
        nameRow.append("th")
            .text(tablename)
            .attr("colspan", data[0].length + links.length)
            .classed("tablename", true)

        let attributeRow = visDiv.append("tr");

        // Add a cell for each item in the schema list
        attributeRow.selectAll("th.schema")
            .data(schema)
            .enter()
            .append("th")
            .text((d) => d)
            .classed("dschemaata", true);  

        // Append a row for each element in the data array
        var rows = visDiv.selectAll("tr.data")
            .data(data)
            .enter()
            .append("tr")
            .classed("data", true);        

        // Append a cell for each element in the row array
        var cells = rows.selectAll("td")
            .data(function (d) { return d; })
            .enter()
            .append("td")

        cells.text(function (d) { return d; });

        // Add a mouseover event to the cells
        cells.on("mouseover", function () {
            // Get the cell value
            const cellValue = d3.select(this).text();
            // Show the tooltip and set its position and content
            d3.select(".tooltip")
                .style("display", "block")
                .style("left", event.pageX + 10 + "px")
                .style("top", event.pageY + 10 + "px")
                .html(`${cellValue}`);
        });

        // Add a mouseout event to the cells
        cells.on("mouseout", function () {
            // Hide the tooltip
            d3.select(".tooltip")
                .style("display", "none");
        });

        // Append the links in the schema array
        attributeRow.selectAll("th.link")
            // include row data information into the data
            .data(function (d) { 
                // create deep copy
                let linksCopy = JSON.parse(JSON.stringify(Array.from(links)));
                return linksCopy})
            .enter()
            .append("th")
            .text(function (d) {return d.target.id === tablename? d.source.name :d.target.name;})
            .classed("link", true);

        // Append the links in the row array
        var linkCell = rows.selectAll("td.link")
            .data(function (d) { 
                let linksCopy = JSON.parse(JSON.stringify(Array.from(links)));
                // each link store additional information about tuple, schema and table
                // these are necessary for exploration
                return linksCopy.map(link => {link["d"]=d;link["schema"]=schema;link["tablename"]=tablename;return link}); })
            .enter()
            .append("td")
            .classed("link", true)
            .append("button")
            .text("explore")
            .on("click", function(d,i){
                console.log(d)
            });

    }
}
