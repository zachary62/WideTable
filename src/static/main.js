
import JoinGraphView from "./views/joinGraphView.js";
import SchemaView from "./views/schemaView.js";
import VizualizationController from "./controllers/vizualizationController.js";
async function get_graph() {
    return fetch('/get_graph')
        .then(response => response.json())
        .then(data => {return data });
}




build_graph()
async function build_graph() {
    let graph = await get_graph();
    let tableController = new VizualizationController(graph, new JoinGraphView(d3.select('#graphView')), new SchemaView(d3.select('#schemaView')));
}
