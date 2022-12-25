export default class VisView {
    constructor(element, width = 1500, height = 600) {
        this.tableViewElement = element;

        this.width = width;
        this.height = height;
        this.defaultCellHeight = 20
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

    drawDoubleTable(tablename1, schema1, data1, links1,
        tablename2, schema2, data2, links2) {
        drawSingleTable(tablename1, schema1, data1, links1)
        drawSingleTable(tablename2, schema2, data2, links2)
    }

    drawSingleTable(tablename, schema, data, links, cellHeight = -1, visDiv = null, exploreHandler) {
        if (cellHeight == -1) {
            cellHeight = this.defaultCellHeight
        }

        if (visDiv == null) {
            visDiv = this.addVisDiv()
        }

        visDiv = visDiv.append("table")
        // Create a schema row and append it to the table
        let nameRow = visDiv.append("tr")
            // .attr("height", this.defaultCellHeight);

        // Add a row for table name
        nameRow.append("th")
            .text(tablename)
            .attr("colspan", data[0].length + links.length)
            .classed("tablename", true)
            .classed("single", true)

        let attributeRow = visDiv.append("tr")
            // .attr("height", this.defaultCellHeight);

        // Add a cell for each item in the schema list
        attributeRow.selectAll("th.schema")
            .data(schema)
            .enter()
            .append("th")
            .text((d) => d)
            .classed("dschemaata", true)
            .classed("single", true)

        // Append a row for each element in the data array
        var rows = visDiv.selectAll("tr.data")
            .data(data)
            .enter()
            .append("tr")
            .classed("data", true)
            .attr("height", cellHeight);

        // Append a cell for each element in the row array
        var cells = rows.selectAll("td")
            .data(function (d) { return d; })
            .enter()
            .append("td")
            .classed("single", true)
            .classed("single", true)

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
                return linksCopy
            })
            .enter()
            .append("th")
            .text(function (d) { return d.target.id === tablename ? d.source.name : d.target.name; })
            .classed("link", true);

        // Append the links in the row array
        var linkCell = rows.selectAll("td.link")
            .data(function (d) {
                let linksCopy = JSON.parse(JSON.stringify(Array.from(links)));
                // each link store additional information about tuple, schema and table
                // these are necessary for exploration
                return linksCopy.map(link => { link["d"] = d; link["schema"] = schema; link["tablename"] = tablename; return link });
            })
            .enter()
            .append("td")
            .classed("link", true)
            .append("button")
            .text("explore")
            // .on("click", exploreHandler);
            // .on("click", (d)=>exploreHandler(d, this));
            .on("click", function(d) {
                // Select the parent td element of the button
                const cell = d3.select(this.parentNode);
                
                cell.select("button").remove()
                exploreHandler(d, cell)
              })
    }
}
