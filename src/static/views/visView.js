export default class VisView {
    constructor(element, width = 1500, height = 600) {
        this.tableViewElement = element;

        this.width = width;
        this.height = height;
        this.defaultCellHeight = 24
        this.tableCount = 0;
    }

    clear() {
        this.tableViewElement
            .selectAll("div")
            .remove()
        this.visDiv = null;
    }

    addVisDiv() {
        if (this.visDiv == null) {

            this.visDiv = this.tableViewElement
                .append("div")
                .classed("visualization", true)

            this.visDiv.style("width", this.width + "px")
            this.visDiv.style("height", this.height + "px")
        }
        return this.visDiv

    }

    greyOutAllTables(tableIdx) {
        d3.selectAll("table")
            .filter(function(d, i) { return i < tableIdx; })
            .classed("greyed", true)
    }

    // delete this table and all tables after the given index
    deleteTablesAfter(tableIdx) {
        d3.selectAll("table")

            .filter((d, i, elems)  => {
                // generate code to split "table_idx" to get idx value
                let idx = parseInt(elems[i].id.split("_")[1])
                return idx >= tableIdx; })
            .remove()
    }

    drawDoubleTable(tablename1, schema1, data1, links1,
        tablename2, schema2, data2, links2) {
        drawSingleTable(tablename1, schema1, data1, links1)
        drawSingleTable(tablename2, schema2, data2, links2)
    }
    
    /**
     * draw a single table 
     * @param {string} tablename - The header of the table
     * @param {array} schema - A list of attribute names 
     * @param {array} data - A list of tuples, each tuple is also a list of values ordered according to the schema
     * @param {dictory} links - Stores the join key information in the join graph
     * @param {array} [cellHeights=[]] - The height of each cell in the table
     * @param {HTMLElement} [visDiv=null] - The HTML div element to draw the table in
     * @param {function} [exploreHandler=null] - A function to trigger events for table exploration
     */
    drawSingleTable(tablename, schema, data, links, cellHeights = [], visDiv = null, exploreHandler) {
        // if cellHeights is not provided or it's empty, set it to default cell height
        if (cellHeights == null || cellHeights.length === 0) {
            cellHeights = Array(data.length).fill(this.defaultCellHeight);
        } else {
            // otherwise, scale cell heights to default cell height
            cellHeights = cellHeights.map(d => d * this.defaultCellHeight);
        }

        // if no division is given, create a new division for the table
        if (visDiv == null) {
            visDiv = this.addVisDiv()
        }

        this.tableCount+= 1;
        let tableIdx = this.tableCount -1;
        visDiv = visDiv.append("table").attr('id', 'table_' + tableIdx)
        // Create a schema row and append it to the table
        let nameRow = visDiv.append("tr").attr("height", this.defaultCellHeight);

        // Add a row for table name
        nameRow.append("th")
            .text(tablename)
            .attr("colspan", data[0].length + links.length)
            .classed("tablename", true)
            .classed("single", true)
        let attributeRow = visDiv.append("tr")

        // Add a cell for each item in the schema list
        attributeRow.selectAll("th.schema")
            .data(schema)
            .enter()
            .append("th")
            .text((d) => d)
            .attr("height", this.defaultCellHeight)
            .classed("schema", true)
            .classed("single", true)

        // Append a row for each element in the data array
        var rows = visDiv.selectAll("tr.data")
            .data(data)
            .enter()
            .append("tr")
            .classed("data", true)
            .attr("height", function(d, i) {
                return cellHeights[i];
            });

        // Append a cell for each element in the row array
        var cells = rows.selectAll("td")
            .data(function (d) { return d; })
            .enter()
            .append("td")
            .classed("single", true)
            .text(function (d) { return d; });

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
                // console.log(linksCopy)
                return linksCopy
            })
            .enter()
            .append("th")
            .text(function (d) { return d.target.id === tablename ? d.source.name : d.target.name; })
            .classed("link", true);

        // Append the links in the row array
        var linkCell = rows.selectAll("td.link")
            .data((d, i) => {
                let linksCopy = JSON.parse(JSON.stringify(Array.from(links)));
                // each link store additional information about tuple, schema and table
                // these are necessary for exploration
                let filtered = linksCopy.map(link => { link["d"] = d; link["schema"] = schema;
                    link["tableIdx"] = tableIdx;
                    link["tablename"] = tablename; return link });
                // console.log(filtered);
                return filtered;
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
