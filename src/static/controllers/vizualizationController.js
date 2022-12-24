export default class VizualizationController {

    constructor(graph, jGView, schemaView, visView) {
        this.jGView = jGView;
        this.schemaView = schemaView;
        this.visView = visView
        this.jGView.addDragHandlers(this.dragstarted, this.dragged, this.nodeDragEnded, this.edgeDragEnded);
        this.jGView.addBackgroundClickHandler(this.backgroundClickHandler);
        this.jGView.drawGraph(graph);
        this.schemaView.addClickHandler(this.schemaClickHandler);
        this.schemaView.drawSchema(graph);   
    }

    // write handler functions for edge drag and node drag of joinGraphView
    dragstarted(d) {
        // in case we need to handle this in a common way
    }
    dragged(d) {
        // in case we need to handle this in a common way
    }

    nodeDragEnded = async (d) => {
        this.schemaView.unHighlight()
        this.schemaView.highlightRelationSchema(d.id)
        
        this.visView.clear()
        let tablename = d["name"]
        let data = await fetch('/get_relation_sample', {
            method: 'POST',
            headers: {
                'Content-Type': 'text/plain'
            },
            body: tablename
        }).then(response => response.json())
          .then(data => {return data });
          this.visView.drawTable(data["header"], data["data"])
    }
    edgeDragEnded = (d) => {
        this.schemaView.unHighlight()
        d.left_keys[0].map(att => this.schemaView.highlightRelationAttribute(d.source.id, att));
        d.right_keys[0].map(att => this.schemaView.highlightRelationAttribute(d.target.id, att));
        this.schemaView.highlightRelationSchema(d.source.id)
        this.schemaView.highlightRelationSchema(d.target.id)
    }

    schemaClickHandler = (d) => {
        this.jGView.unHighlightGraph()
        d.edges.map((e) => this.jGView.highlightRelationEdge(e.left_rel, e.right_rel, e.color))
        d.relations.map((r) => this.jGView.highlightRelationNode(r.name, r.color, r.text))
    }

    backgroundClickHandler = () => {
        this.jGView.unHighlightGraph()
        this.schemaView.unHighlightSchema()
    }
}