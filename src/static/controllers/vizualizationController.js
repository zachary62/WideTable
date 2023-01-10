import {get_graph} from "../main.js";

export default class VizualizationController {

    constructor(graph, jGView, schemaView, visView, histView, measView) {
        this.jGView = jGView;
        this.schemaView = schemaView;
        this.visView = visView
        this.jGView.addDragHandlers(this.dragstarted, this.dragged, this.nodeDragEnded, this.edgeDragEnded);
        this.jGView.addBackgroundClickHandler(this.backgroundClickHandler);
        this.jGView.drawGraph(graph);
        this.schemaView.addClickHandler(this.schemaClickHandler);
        this.schemaView.addAttributeClickHandler(this.attrClickHandler);
        this.schemaView.drawSchema(graph);
        this.graph = graph;
        this.histView = histView;
        this.measView = measView;
        this.measView.addSubmitHandler(this.measSubmitHandler);
        this.measView.addSelectionChangeHandler(this.measSelectionChangeHandler);
        this.measView.initializeForm(graph);
    }

    refreshAllViewsWithLatestGraph(graph) {
        this.graph = graph;
        this.jGView.drawGraph(graph);
        this.schemaView.drawSchema(graph);
        this.measView.initializeForm(graph);

    }

    getEdges(relationId) {
        let links = this.graph["links"]
        return links.filter(link => link.source.id === relationId || link.target.id === relationId)
    }

    // write handler functions for edge drag and node drag of joinGraphView
    dragstarted(d) {
        // in case we need to handle this in a common way
    }
    dragged(d) {
        // in case we need to handle this in a common way
    }

    getData = async (relation, selection_conds=[], aggregate_exprs=null,groupby_conds=null, orderby_conds=null,limit=100) => {
        let input = {
            relation: relation,
            selection_conds: selection_conds,
            groupby_conds: groupby_conds,
            orderby_conds: orderby_conds,
            agg_exprs: aggregate_exprs,
            limit: limit
        }
        let data = await fetch('/get_relation_sample', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(input)
        }).then(response => response.json())
            .then(data => { return data });

        return data
    }

    getHistData = async (relation, attr) => {
        let aggregate_expressions = {'count':[`${attr}`, 'COUNT'], attr:[`${attr}`, 'IDENTITY']}
        let groupby_conds = [`${attr}`]
        let orderby_conds = [`COUNT(${attr}) DESC`]
        return await this.getData(relation, [], aggregate_expressions, groupby_conds, orderby_conds,5)
    }
    addMeasurement = async (relation, attr, scope, agg) => {
        let input = {
            relation: relation,
            attr: attr,
            scope: scope,
            agg: agg
        }
        let data = await fetch('/add_measurement', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(input)
        }).then(response => response.json())
            .then(data => {return data; });
        return data
    }

    nodeDragEnded = async (d) => {
        this.schemaView.unHighlight()
        this.schemaView.highlightRelationSchema(d.id)

        this.visView.clear()
        let tablename = d["name"]

        let data = await this.getData(tablename)

        let links = this.getEdges(d["id"])
        this.visView.clear()
        this.visView.drawSingleTable(tablename, data["header"], data["data"], links, null, null, this.exploreHandler)
    }
    edgeDragEnded = (d) => {
        this.schemaView.unHighlight()
        d.left_keys[0].map(att => this.schemaView.highlightRelationAttribute(d.source.id, att));
        d.right_keys[0].map(att => this.schemaView.highlightRelationAttribute(d.target.id, att));
        this.schemaView.highlightRelationSchema(d.source.id)
        this.schemaView.highlightRelationSchema(d.target.id)
    }

    // exploreHandler = async (d) => {
    //     let data = d["d"]
    //     let schema = d["schema"]
    //     let tablename =  d["tablename"]
    //     console.log(d, data, schema, tablename)

    //     let cur_join_keys = tablename === d.source.id ? d.left_keys[0]: d.left_keys[1]
    //     let next_join_keys = tablename === d.source.id ? d.left_keys[1]: d.left_keys[0]
    //     let next_tablename = tablename === d.source.id ? d.target.name: d.source.name
    //     let cur_selection_conds = cur_join_keys.map(key => key + " = " + data[schema.indexOf(key)])
    //     let next_selection_conds = next_join_keys.map((key, idx) => key + " = " + data[schema.indexOf(cur_join_keys[idx])])

    //     this.visView.clear()
    //     let visDiv = this.visView.addVisDiv()

    //     let data1 = await this.getData(tablename, cur_selection_conds)
    //     let data2 = await this.getData(next_tablename, next_selection_conds)

    //     let links1 = this.getEdges(tablename)
    //     let links2 = this.getEdges(next_tablename)
        
    //     this.visView.drawSingleTable(tablename, data1["header"], data1["data"], links1, null, visDiv, this.exploreHandler)
    //     this.visView.drawSingleTable(next_tablename, data2["header"], data2["data"], links2, null, visDiv, this.exploreHandler)

    // }

    exploreHandler = async (d, element) => {
        let data = d["d"]
        let schema = d["schema"]
        let tablename =  d["tablename"]

        console.log(element)
        let cur_join_keys = tablename === d.source.id ? d.left_keys[0]: d.left_keys[1]
        let next_join_keys = tablename === d.source.id ? d.left_keys[1]: d.left_keys[0]
        let next_tablename = tablename === d.source.id ? d.target.name: d.source.name
        let cur_selection_conds = cur_join_keys.map(key => key + " = " + data[schema.indexOf(key)])
        let next_selection_conds = next_join_keys.map((key, idx) => key + " = " + data[schema.indexOf(cur_join_keys[idx])])

        // this.visView.clear()
        // let visDiv = this.visView.addVisDiv()

        // let data1 = await this.getData(tablename, cur_selection_conds)
        let data2 = await this.getData(next_tablename, next_selection_conds)

        let links1 = this.getEdges(tablename)
        let links2 = this.getEdges(next_tablename)
        
        // this.visView.drawSingleTable(tablename, data1["header"], data1["data"], links1, null, visDiv, this.exploreHandler)
        this.visView.drawSingleTable(next_tablename, data2["header"], data2["data"], links2, null, element, this.exploreHandler)

    }

    schemaClickHandler = (d) => {
        this.jGView.unHighlightGraph()
        d.edges.map((e) => this.jGView.highlightRelationEdge(e.left_rel, e.right_rel, e.color))
        d.relations.map((r) => this.jGView.highlightRelationNode(r.name, r.color, r.text))
    }
    attrClickHandler = async (d, i, elems) => {
        let relationName = elems[i].parentNode.__data__.name
        let data = await this.getHistData(relationName, d)
        this.histView.clear()
        this.histView.drawHistogram(data,null, relationName, d)
    }

    backgroundClickHandler = () => {
        this.jGView.unHighlightGraph()
        this.schemaView.unHighlightSchema()
    }

    measSelectionChangeHandler = (relation)=> {
        // console.log(this.graph)
        let node = this.graph.nodes.find((n) => n.name === relation)
        // console.log(node);
        this.measView.populateAttributeDropdown(node)
    }

    measSubmitHandler = async (relation, attribute, aggregate, scope) => {
        let response = await this.addMeasurement(relation, attribute, aggregate, scope)
        if (Object.keys(response).includes('error')) {
            this.measView.clearErrorMessage();
            this.measView.displayFormErrorMessage(response.error);
            return;
        }
        let graph = await get_graph()
        this.refreshAllViewsWithLatestGraph(graph)
    }
}