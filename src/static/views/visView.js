export default class VisView {
    constructor(element, width = 600, height = 300) {
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

    drawTable(header, data) {
        this.clear()
        var visDiv = this.addVisDiv()

        // Create a header row and append it to the table
        const headerRow = visDiv.append("tr");

        // Add a cell for each item in the header list
        headerRow.selectAll("th")
        .data(header)
        .enter()
        .append("th")
        .text((d) => d);
        
        // Append a row for each element in the data array
        var rows = visDiv.selectAll("tr")
            .data(data)
            .enter()
            .append("tr");

        // Append a cell for each element in the row array
        var cells = rows.selectAll("td")
            .data(function (d) { return d; })
            .enter()
            .append("td")
        
        cells.text(function (d) { return d; });

        // Add a mouseover event to the cells
        cells.on("mouseover", function() {
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
        cells.on("mouseout", function() {
            // Hide the tooltip
            d3.select(".tooltip").style("display", "none");
        });
        }
}
