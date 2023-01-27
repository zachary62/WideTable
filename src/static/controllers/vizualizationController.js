import { get_graph } from "../main.js";

export default class VizualizationController {
    // Initialization function that draws the web page given the join graph data and views classes
    constructor(graph, jGView, schemaView, visView, histView, measView) {
        // The graph is from the server and contains the schema-level information of the nodes and edges of the join graph
        this.graph = graph;

        // Create views for the join graph, schema side bar, measurement side bar, visualization, histogram, and 
        this.jGView = jGView;
        this.schemaView = schemaView;
        this.measView = measView;
        this.visView = visView;
        this.histView = histView;

        // These are for the drag events of join graph node and edge
        this.jGView.addDragHandlers(this.dragstarted, 
                                    this.dragged, 
                                    this.nodeDragEnded, 
                                    this.edgeDragEnded);
        // These are for the event of clicking the background
        this.jGView.addBackgroundClickHandler(this.backgroundClickHandler);
        // Draw the join graph based on the graph
        this.jGView.drawGraph(graph);

        // Add handler when clicking on a table schema
        this.schemaView.addClickHandler(this.schemaClickHandler);
        // Add handler when clicking on a table attribute
        this.schemaView.addAttributeClickHandler(this.attrClickHandler);
        // Draw the schema based on the graph
        this.schemaView.drawSchema(graph);

        // Add handler when clicking on the submit measurement button
        this.measView.addSubmitHandler(this.measSubmitHandler);
        // Add handler when changing the selection
        this.measView.addSelectionChangeHandler(this.measSelectionChangeHandler);
        // Initialize the measurement form based on the graph
        this.measView.initializeForm(graph);
}


    // update the views when the join graph has been updated
    refreshAllViewsWithLatestGraph(graph) {
        this.graph = graph;
        this.jGView.drawGraph(graph);
        this.schemaView.drawSchema(graph);
        this.measView.initializeForm(graph);
    }

    // given the relation Id, return the edge id in the join graph 
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

    getData = async (relation, selection_conds = [], aggregate_exprs = null, groupby_conds = [],
        orderby_conds = [], limit = 100, custom_order_pref = []) => {
        let input = {
            relation: relation,
            selection_conds: selection_conds,
            groupby_conds: groupby_conds,
            orderby_conds: orderby_conds,
            agg_exprs: aggregate_exprs,
            custom_order_pref: custom_order_pref,
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

    getHistData = async (relation, attr, limit = 5) => {
        let aggregate_expressions = { 
            'count': [`${attr}`, 'COUNT'], 
            attr: [`${attr}`, 'IDENTITY'] 
        }
        let groupby_conds = [`${attr}`]
        let orderby_conds = [`COUNT(${attr}) DESC`]
        return await this.getData(relation, undefined, aggregate_expressions, groupby_conds, orderby_conds, limit)
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
            .then(data => { return data; });
        return data
    }

    nodeDragEnded = async (d) => {
        // highlight the relation dragged
        this.schemaView.unHighlight()
        this.schemaView.highlightRelationSchema(d.id)

        // visualize the table of the relation
        this.visView.clear()
        let tablename = d["name"]
        let orderby_conds = d["join_keys"];
        let groupby_conds = undefined;
        let measurements = d["measurements"];
        let aggregate_expressions = {}
        groupby_conds = this.generateAggExprsAndGroupByConds(measurements, aggregate_expressions, d["join_keys"]);
        let data = await this.getData(tablename, undefined, aggregate_expressions, groupby_conds, orderby_conds)

        // draw the new table
        let links = this.getEdges(d["id"])
        this.visView.clear()
        this.visView.drawSingleTable(tablename, data["header"], data["data"], links, undefined, undefined, this.exploreHandler)
    }

    generateAggExprsAndGroupByConds(measurements, aggregate_expressions, join_keys) {
        let groupby_conds = []
        if (measurements.length > 0) {
            measurements.map(meas => {
                aggregate_expressions[`${meas["agg"]}_${meas['attr'].replace(/[ \*-+\/]/g, "_")}`] = [meas["attr"], meas["agg"]]
            });
            groupby_conds = join_keys;
            groupby_conds.map(grp => {
                aggregate_expressions[grp] = [grp, "IDENTITY"]
            })
        }
        return groupby_conds;
    }

    edgeDragEnded = (d) => {
        this.schemaView.unHighlight()
        d.left_keys[0].map(att => this.schemaView.highlightRelationAttribute(d.source.id, att));
        d.right_keys[0].map(att => this.schemaView.highlightRelationAttribute(d.target.id, att));
        this.schemaView.highlightRelationSchema(d.source.id)
        this.schemaView.highlightRelationSchema(d.target.id)
    }

    // This handler expand the tuple like a json, which fails for many-to-many
    exploreNestedHandler = async (d, element) => {
        let data = d["d"]
        let schema = d["schema"]
        let tablename =  d["tablename"]

        console.log(element)
        let cur_join_keys = tablename === d.source.id ? d.left_keys[0]: d.left_keys[1]
        let next_join_keys = tablename === d.source.id ? d.left_keys[1]: d.left_keys[0]
        let next_tablename = tablename === d.source.id ? d.target.name: d.source.name
        let next_selection_conds = next_join_keys.map((key, idx) => key + " = " + `'${data[schema.indexOf(cur_join_keys[idx])]}'`)

        // this.visView.clear()
        // let visDiv = this.visView.addVisDiv()

        // let data1 = await this.getData(tablename, cur_selection_conds)
        let data2 = await this.getData(next_tablename, next_selection_conds)

        let links2 = this.getEdges(next_tablename)
        
        // this.visView.drawSingleTable(tablename, data1["header"], data1["data"], links1, null, visDiv, this.exploreHandler)
        this.visView.drawSingleTable(next_tablename, data2["header"], data2["data"], links2, null, element, undefined, this.exploreHandler)

    }

    // This handler expands the tuple by sorting the table by the join key.
    // The variable "d" stores data, which is the tuple (a list of values) of the selected tuple.
    // "d" also stores the schema, which is a list of attributes, the table name as a string, and the table index as a single integer.
    exploreHandler = async (d, element) => {
        let data = d["d"]
        let schema = d["schema"]
        let tablename = d["tablename"]
        let tableIdx = d["tableIdx"]

        // The join key is on two sides. 
        // "cur_join_key" is from the current table
        // "next_join_key" is from the table to be explored.
        let cur_join_keys = tablename === d.source.id ? d.left_keys[0] : d.left_keys[1]
        let next_join_keys = tablename === d.source.id ? d.left_keys[1] : d.left_keys[0]
        let next_tablename = tablename === d.source.id ? d.target.name : d.source.name

        let cur_measurements = this.graph.nodes.filter(node => node.name === tablename)[0].measurements;
        // generate current aggregation expressions
        let cur_agg_exprs = {}
        let cur_groupby_conds = this.generateAggExprsAndGroupByConds(cur_measurements, cur_agg_exprs , cur_join_keys);
        if (cur_measurements.length > 0) {
            cur_measurements.map(meas => {
                cur_agg_exprs[meas["agg"]] = [meas["attr"], meas["agg"]]
            });
            cur_groupby_conds = d["source"]["join_keys"];
            cur_groupby_conds.map(grp => {
                cur_agg_exprs[grp] = [grp, "IDENTITY"]
            })
        }

        // create selection condition of join table
        //  {col} = '{custom_order_pref[i]}' DESC, {col}
        // order by key = value desc, key
        let selected_join_values = cur_join_keys.map(key => data[schema.indexOf(key)])
        let leftTableData = await this.getData(tablename, undefined, cur_agg_exprs, cur_groupby_conds, cur_join_keys, undefined, selected_join_values)
        let projection = {}

        // create a single of join key. This is to better connect two tables.
        cur_join_keys.map(key => projection[key] = [key, 'IDENTITY'])
        let joinTable = await this.getData(tablename, undefined, projection, undefined, cur_join_keys, undefined, selected_join_values)

        // "links" stores the join relationship data.
        let links1 = this.getEdges(tablename)
        let links2 = this.getEdges(next_tablename)

        // this get, for the current table, what are the unique join key values
        let cur_join_key_idxs = cur_join_keys.map(key => leftTableData["header"].indexOf(key))
        let cur_join_key_tuples = leftTableData["data"].map(row => cur_join_key_idxs.map(idx => row[idx]))
        let cur_join_key_set = new Set(cur_join_key_tuples.map(JSON.stringify))

        // get right table data but filter for only join key values
        let next_selection_conds = Array.from(cur_join_key_set).map(JSON.parse).map(tuple => tuple.map((key, idx) => next_join_keys[idx] + " = " + `'${key}'`).join(" AND ")).join(" OR ")
        let next_measurements = this.graph.nodes.filter(node => node.name === next_tablename)[0].measurements;
        let next_groupby = undefined
        let next_agg_exprs = {}
        next_groupby = this.generateAggExprsAndGroupByConds(next_measurements, next_agg_exprs, next_join_keys)

        let rightTableData = await this.getData(next_tablename, [next_selection_conds], next_agg_exprs, next_groupby, next_join_keys, undefined, selected_join_values)

        // calculates cell heights of left table rows, right table rows and join row cell height
        let next_join_key_idxs = next_join_keys.map(key => rightTableData["header"].indexOf(key))
        let cellHeights = []
        let leftcellHeights = []
        let rightCellHeights = []
        function calculateCellHeights() {
            // for each value of the join key
            Array.from(cur_join_key_set).map(JSON.parse).forEach(key_tuple => {
                // l_count is the number of tuples in current table with this join key value
                let l_count = cur_join_key_tuples.filter(k => JSON.stringify(k) === JSON.stringify(key_tuple)).length
                // r_count is the number of tuples in current table with this join key value
                let r_count = rightTableData["data"].filter(row => JSON.stringify(next_join_key_idxs.map(idx => row[idx])) === JSON.stringify(key_tuple)).length
                let max_count = Math.max(l_count, r_count)
                cellHeights.push(max_count)
                leftcellHeights.push(...Array(l_count).fill(max_count / l_count))
                rightCellHeights.push(...Array(r_count).fill(max_count / r_count))
            });
        }

        calculateCellHeights();

        // grey out all existing tables in visView
        this.visView.greyOutAllTables(tableIdx);
        // delete all tables after the current table (including the current table) so we can redraw them
        this.visView.deleteTablesAfter(tableIdx);
        // redraw left table
        this.visView.drawSingleTable(tablename, leftTableData["header"], leftTableData["data"], links1, leftcellHeights, undefined, this.exploreHandler)
        // draw join table
        this.visView.drawSingleTable("Join Keys", joinTable["header"], Array.from(cur_join_key_set).map(JSON.parse), [], cellHeights, undefined, undefined, this.exploreHandler)
        // draw right table
        this.visView.drawSingleTable(next_tablename, rightTableData["header"], rightTableData["data"], links2, rightCellHeights, undefined, this.exploreHandler)

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
        this.histView.drawHistogram(data, null, relationName, d)
    }

    backgroundClickHandler = () => {
        this.jGView.unHighlightGraph()
        this.schemaView.unHighlightSchema()
    }

    measSelectionChangeHandler = (relation) => {
        let node = this.graph.nodes.find((n) => n.name === relation)
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