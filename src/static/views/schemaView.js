export default class SchemaView {
    constructor(element) {
        this.schemaViewElement = element;
    }

    addClickHandler(handler) {
        this.clickHandler = handler
    }

    unHighlight() {
        this.unExpandSchema()
        this.unHighlightSchema()
    }

    unHighlightSchema() {
        this.schemaViewElement.selectAll("span")
            .classed('highlight', false)

    }

    unExpandSchema() {
        this.schemaViewElement.selectAll("span")
            .classed('expanded', false)
    }

    highlightRelationAttribute(relation, attribute) {
        console.log(relation, attribute)
        this.schemaViewElement
            .select("#schema_" + relation)
            .select("#attribute_" + attribute)
            .classed('highlight', true)
    }

    highlightRelationSchema(relation) {
        // highlight schema
        let schema = d3.select("#schema_" + relation)
        schema
            .selectAll('div')
            .selectAll('span')
            .classed('expanded', true)

        // flash schema
        schema
            .transition()
            .duration(0)
            .style("opacity", 0)
            .transition()
            .duration(2000)
            .style("opacity", 100);
    }

    drawSchema(graph) {
        // add relations
        var schema = this.schemaViewElement
            .selectAll("div")
            .data(graph.nodes)
            .enter()
            .append("div")
            .attr('class', 'relation')
            .attr('id', (d) => { return 'schema_' + d.name; })
            .on("click", function (d) {
                d3.select(this)
                    .selectAll("span")
                    .classed('expanded', function () {
                        return !d3.select(this).classed("expanded");
                    })
            });
        schema
            .append("text")
            .attr('class', 'relationName')
            .text((d) => { return d.id; })

        // add measurement attributes
        schema
            .append("div")
            .selectAll("span")
            .data((d) => { return d.measurements; })
            .enter()
            .append("span")
            .attr('id', (d) => { return 'attribute_' + d.name; })
            .attr('class', 'attribute measurements')
            .text((d) => { return d.name; })
            .on("click", (d, i, elems) => {
                // this stopPropagation is to prevent the expanding of parent node
                d3.event.stopPropagation();
                this.unHighlightSchema()
                d3.select(elems[i]).classed('highlight', true)
                this.clickHandler(d)
            });

        // add join attributes
        schema
            .append("div")
            .selectAll("span")
            .data((d) => { return d.join_keys; })
            .enter()
            .append("span")
            .attr('id', (d) => { return 'attribute_' + d; })
            .attr('class', 'attribute join_keys')
            .text((d) => { return d; })
            .on("click", function (d) {
                d3.event.stopPropagation();
            });

        // add dimension attributes
        schema
            .append("div")
            .selectAll("span")
            .data((d) => { return d.attributes; })
            .enter()
            .append("span")
            .attr('id', (d) => { return 'attribute_' + d; })
            .attr('class', 'attribute')
            .text((d) => { return d; })
            .on("click", function (d) {
                d3.event.stopPropagation();
            });

        this.schemaViewElement.on("mousedown", (d, i, nodes) => {
            this.unHighlightSchema();
        })
    }


}
