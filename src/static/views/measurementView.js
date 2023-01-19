export default class MeasurementView {
    constructor(element) {
        this.measurementViewElement = element;
    }

    addSubmitHandler(handler) {
        this.submitHandler = handler;
    }

    addSelectionChangeHandler(handler) {
        this.selectionChangeHandler = handler;
    }

    clearErrorMessage() {
        d3.select('#error-message').text('');
    }

    // display error message in the view
    displayFormErrorMessage(message) {
        d3.select("#error-message").text(message);
    }

    initializeForm(graph) {
        //clear the view
        this.measurementViewElement.selectAll("*").remove();
        // Create the form container
        var form = this.measurementViewElement
                       .append("form")
                       .attr("class", "measurementForm");

        // Add the relation dropdown
        form.append("label")
            .attr("for", "relation-dropdown")
            .text("Relation: ");
        form.append("select")
            .attr("id", "relation-dropdown")
            .selectAll("option")
            .data(graph.nodes)
            .enter().append("option")
            // select first relation by default
            .attr("selected", (d, i) => { return i === 0 ? "selected" : null; })
            .text(function (d) {
                return d.name;
            });
        // attach event listener to relation dropdown for any change in selected relation
        d3.select("#relation-dropdown").on("change", (d,i,elems) => {
            this.selectionChangeHandler(elems[i].selectedOptions[0].text);
        });
        form.append("br");
        form.append("br");

        // Add the scope dropdown
        form.append("label")
            .attr("for", "scope-dropdown")
            .text("Scope: ");
        form.append("select")
            .attr("id", "scope-dropdown")
            .selectAll("option")
            .data(["FullJoin", "Single","ReplicateFact", "AverageAttribution"])
            .enter().append("option")
            .text(function (d) {
                return d;
            });
        form.append("br");
        form.append("br");

        // Add the aggregation function dropdown
        form.append("label")
            .attr("for", "agg-function-dropdown")
            .text("Aggregation: ");
        form.append("select")
            .attr("id", "agg-function-dropdown")
            .selectAll("option")
            .data(["Count", "Sum", "Avg"])
            .enter().append("option")
            .text(function (d) {
                return d;
            });
        form.append("br");
        form.append("br");

        // Add a toggle button for making visible the custom attribute input
        form.append("label")
            .attr("for", "custom-attr-toggle")
            .text("Custom Expression?: ");
        form.append("input")
            .attr("type", "checkbox")
            .attr("id", "custom-attr-toggle")
            .attr("name", "custom-attr-toggle")
            .attr("value", "custom-attr-toggle")
            .on("change", (d, i, elems) => {
                if (elems[i].checked) {
                    d3.select("#custom-attr-input").style("visibility", "visible");
                    d3.select('#attribute-dropdown').style("visibility", "hidden");
                    d3.select('#attribute-label').style("visibility", "hidden");
                } else {
                    d3.select("#custom-attr-input").style("visibility", "hidden");
                    d3.select('#attribute-dropdown').style("visibility", "visible");
                    d3.select('#attribute-label').style("visibility", "visible");
                }
            });
        form.append("br");
        form.append("br");

        // Add the attribute dropdown
        form.append("label")
            .attr('id', 'attribute-label')
            .attr("for", "attribute-dropdown")
            .text("Attribute: ");
        form.append("select")
            .attr("id", "attribute-dropdown")
            .selectAll("option")
            .data(graph.nodes[0].attributes)
            .enter().append("option")
            .text(function (d) {
                return d;
            });
        //Add a text input for custom attribute
        form.append("input")
            .attr("id", "custom-attr-input")
            .attr("type", "text")
            .attr("placeholder", "Enter custom expression")
            .style("visibility", "hidden");

        form.append("br");
        form.append("br");

        // Add the submit button
        form.append("input")
            .attr('id', 'submit-button')
            .attr("type", "submit")
            .attr("value", "Add measurement");

        form.append("br");
        form.append("br");
        // Add a span to display error messages
        form.append("span")
            .attr("id", "error-message")
            .attr("class", "error");

        // attach event listener to submit button
        d3.select("#submit-button").on("click", (d, i, elems) => {
            d3.event.stopPropagation();
            d3.event.preventDefault();
            let relation = d3.select("#relation-dropdown").node().value;
            let scope = d3.select("#scope-dropdown").node().value;
            let aggFunction = d3.select("#agg-function-dropdown").node().value;
            let customExprToggle = d3.select("#custom-attr-toggle").node().checked;
            let attribute = d3.select("#attribute-dropdown").node().value;
            if (customExprToggle) {
                attribute = d3.select("#custom-attr-input").node().value;
            }
            this.submitHandler(relation, attribute, scope, aggFunction);
        });
    }

    populateAttributeDropdown(relation) {
        d3.select("#attribute-dropdown")
            .selectAll("option")
            .data(relation.attributes)
            .text(function (d) {
                return d;
            });
    }
}
