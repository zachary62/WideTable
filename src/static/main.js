
import JoinGraphView from "./views/joinGraphView.js";
import SchemaView from "./views/schemaView.js";
import VizualizationController from "./controllers/vizualizationController.js";
import VisView from "./views/visView.js";
import HistView from "./views/histView.js";


async function get_graph() {
    return fetch('/get_graph')
        .then(response => response.json())
        .then(data => {return data });
}

async function build_graph() {
    let graph = await get_graph();
    let tableController = new VizualizationController(graph, 
        new JoinGraphView(d3.select('#graphView')), 
        new SchemaView(d3.select('#schemaView')),
        new VisView(d3.select('#visView')),
        new HistView(d3.select('#histView'))
    );
}

build_graph()

