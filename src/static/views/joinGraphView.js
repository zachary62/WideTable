
export default class JoinGraphView {
    constructor(element) {
        this.graphViewElement = element;

        this.width = 600;
        this.height = 600;

        // append the svg object to the body of the page
        this.svg = this.graphViewElement
            .append('svg')
            .attr('width', this.width)
            .attr('height', this.height)

        this.circleRadius = 30
        // console.log(this.circleRadius)
        this.linkDistance = 120
        this.triangleBase = 15
        this.triangleHeight = 13
        //svg elements can be clipped and marker defs are rendered in the corner for some reason
        //this offset will move it out of the corner
        this.verticalOffset = 5
        // this is to make the edges a bit longer.
        this.edgeLengthModifier = 3
    }

    //Add binding methods for edge and node click events

    addDragHandlers(startHandler, dragHandler, nodeEndDragHandler, edgeEndDragHandler) {
        this.startHandler = startHandler;
        this.dragHandler = dragHandler;
        this.nodeDragEndHandler = nodeEndDragHandler;
        this.edgeEndDragHandler = edgeEndDragHandler;
    }
    addBackgroundClickHandler(handler) {
        this.backgroundClickHandler = handler;
    }

    unHighlightGraph() {
        this.node
            .classed('highlight',false)
            .style('color', null)

        this.link
            .classed('highlight',false)
            .attr('marker-end', function(d) { if (d.multiplicity[1]==='M') return 'url(#arrowHead)'; else return '';})
            .attr('marker-start', function(d) { if (d.multiplicity[0]==='M') return 'url(#arrowHead)'; else return '';})
            .style('color', null)

        this.clickableLinks.classed('highlight',false);

        this.nodeAboveText
            .text("")
    }


    unHighlight() {
        this.unHighlightGraph();
    }

    highlightRelationNode(relation, color=null, text="") {
        // highlight node
        d3.select("#circle_" + relation)
            .classed('highlight',true)
            .style('color', color)

        d3.select("#circleText_" + relation)
            .text(text)
    }

    highlightRelationEdge(sourceRelation, targetRelation, color=null) {
        d3.select("#edge_" + sourceRelation + "-" + targetRelation)
            .classed('highlight',true)
            .attr('marker-end', function(d) { if (d.multiplicity[1]==='M') return 'url(#arrowHeadHighlight)'; else return '';})
            .attr('marker-start', function(d) { if (d.multiplicity[0]==='M') return 'url(#arrowHeadHighlight)'; else return '';})
            .style('color', color)
        d3.select("#clickable_edge_" + sourceRelation + "_" + targetRelation)
            .classed('highlight',true)
    }

    dragstarted(d, simulation) {
        // console.log(d)
        if (!d3.event.active) this.simulation.alphaTarget(0.3).restart();
        this.startHandler(d);
    }

    dragged(d, i) {
        d.fx = d3.event.x;
        d.fy = d3.event.y;
        this.dragHandler(d);
    }


    nodedragended(d, simulation) {
        console.log(d)
        if (!d3.event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
        this.nodeDragEndHandler(d);
    }

    edgedragended(d, simulation) {
        if (!d3.event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
        this.edgeEndDragHandler(d);
    }

    drawGraph(graph) {
        // this.clearGraph();
        var defs = this.svg.append('defs')

        defs.append('marker')
            .attr('id','arrowHead').attr('markerWidth','30').attr('markerHeight','30')
            .attr('refX', this.circleRadius + this.triangleHeight-3).attr('refY',this.triangleBase/2 + this.verticalOffset)
            .attr('orient', 'auto-start-reverse')
            .attr('class','markers')
            .attr('markerUnits', 'userSpaceOnUse')
            .append('polyline')
            // triangle ArrowHead
            .attr('transform', `translate(0,${this.verticalOffset})`)
            .attr('points',`0 ${this.triangleBase/2}, ${this.triangleHeight +this.edgeLengthModifier} 0, 0 ${this.triangleBase/2}, ${this.triangleHeight +this.edgeLengthModifier} ${this.triangleBase}`)
        // .attr('points',`0 0, ${triangleHeight} ${triangleBase/2}, 0 ${triangleBase}`)

        defs.append('marker')
            .attr('id','arrowHeadHighlight').attr('markerWidth','100px').attr('markerHeight','100px')
            .attr('refX', this.circleRadius + this.triangleHeight-3).attr('refY',this.triangleBase/2 + this.verticalOffset)
            .attr('orient', 'auto-start-reverse').attr('class', 'markers highlight')
            .attr('markerUnits', 'userSpaceOnUse')
            .attr('transform', `translate(0,${this.verticalOffset})`)
            .append('polyline')
            .attr('class', 'markers highlight')
            // triangle ArrowHead
            .attr('transform', `translate(0,${this.verticalOffset})`)
            .attr('points',`0 ${this.triangleBase/2}, ${this.triangleHeight +this.edgeLengthModifier} 0, 0 ${this.triangleBase/2}, ${this.triangleHeight +this.edgeLengthModifier} ${this.triangleBase}`)

        this.simulation = d3.forceSimulation()
            .force("link", d3.forceLink().id((d)=>{return d.id;}))
            .force("charge", d3.forceManyBody().strength(-280))
            .force("center", d3.forceCenter(this.width / 2, this.height / 2));


        //list of link containers
        let linkContainers = this.svg.append('g')
            .attr('class', 'links')
            .selectAll("g")
            .data(graph.links)
            .enter()
            .append("g");

        var edgeDragEvent = d3.drag()
            .on("start", (d,i, nodes) => {this.dragstarted(d, this.simulation)})
            .on("drag", (d,i, nodes) => {this.dragged(d, this.simulation)})
            .on("end", (d,i,nodes) => {
                // console.log(d.source.id)
                this.unHighlight();
                this.edgedragended(d, this.simulation);
                this.highlightRelationNode(d.source.id)
                this.highlightRelationNode(d.target.id)
                this.highlightRelationEdge(d.source.id, d.target.id);
            })

        this.link = linkContainers
            .append("line")
            .attr('id', (d)=>{return "edge_" + d.source + "-" + d.target;})
            .attr('marker-end', function(d) { if (d.multiplicity[1]==='M') return 'url(#arrowHead)'; else return '';})
            .attr('marker-start', function(d) { if (d.multiplicity[0]==='M') return 'url(#arrowHead)'; else return '';})
            .call(edgeDragEvent);
        
        this.clickableLinks = linkContainers
            .append('line')
            .attr('id', (d) => {return 'clickable_edge_' + d.source + '_' + d.target})
            .attr('x1', function(d){return d.source.x;})
            .attr('y1', function(d){return d.source.y;})
            .attr('x2', function(d){return d.target.x;})
            .attr('y2', function(d){return d.target.y;})
            .attr('class', 'clickable_edge')
            .call(edgeDragEvent);
        var linkTexts = linkContainers
        .append('text')
        .attr('x',(d)=>{return d.source.x;})
        .attr('y',(d)=>{return d.source.y;})
        .text((d, i)=>{return 'txt';})
        .call(edgeDragEvent)


        //list of node containers
        var dataEnter = this.svg
            .append("g")
            .selectAll("g")
            .data(graph.nodes)
            .enter()
            .append("g")
            .attr("class", "nodes")
            .attr('id', (d)=>{return 'node_container_' + d.id});

        var nodeDragEvent = d3.drag()
            .on("start", (d,i, nodes) => {this.dragstarted(d, this.simulation)})
            .on("drag", (d,i, nodes) => {this.dragged(d, this.simulation)})
            .on("end", (d, i, nodes) => {
                this.unHighlight();
                this.nodedragended(d, this.simulation);
                this.highlightRelationNode(d.id);
            })

        this.node = dataEnter.append("circle")
            .attr('id', (d)=>{return "circle_" + d.id;})
            .attr("r", this.circleRadius)
            .call(nodeDragEvent);

        var nodeText = dataEnter.append("text")
            .attr("dy", "1em")
            .attr('fill', 'white')
            .text((d)=>{return d.name;})
            .call(nodeDragEvent);

        this.nodeAboveText = dataEnter.append("text")
            .attr('id', (d)=>{return "circleText_" + d.id;})
            .attr("dy", -this.circleRadius)
            .attr('fill', 'black')
            .call(nodeDragEvent)


        this.nodeAboveText.style('font-family','cursive');

        function ticked() {
            let radius = this.circleRadius
            let w = this.width
            let h = this.height
            // make sure that all the links are within boundary
            this.link.attr("x1", (d)=>{
                let nodeX = Math.max(this.circleRadius, Math.min(this.width-this.circleRadius, d.source.x))
                return Math.max(0, Math.min(this.width, nodeX));
            })
                .attr("y1", (d)=>{
                    let nodeY = Math.max(this.circleRadius, Math.min(this.height-this.circleRadius, d.source.y))
                    // console.log(this.circleRadius)
                    return Math.max(0, Math.min(this.height, nodeY));
                })
                .attr("x2", (d)=>{
                    let nodeX = Math.max(this.circleRadius, Math.min(this.width-this.circleRadius, d.target.x))
                    return Math.max(0, Math.min(this.width, nodeX));
                })
                .attr("y2", (d)=>{
                    let nodeY = Math.max(this.circleRadius, Math.min(this.height-this.circleRadius, d.target.y))
                    return Math.max(0, Math.min(this.height, nodeY));
                });

            this.clickableLinks.attr("x1", (d)=>{
                let nodeX = Math.max(this.circleRadius, Math.min(this.width-this.circleRadius, d.source.x))
                return Math.max(0, Math.min(this.width, nodeX));
            })
                .attr("y1", (d)=>{
                    let nodeY = Math.max(this.circleRadius, Math.min(this.height-this.circleRadius, d.source.y))
                    return Math.max(0, Math.min(this.height, nodeY));
                })
                .attr('x2', (d)=>{
                    let nodeX = Math.max(this.circleRadius, Math.min(this.width-this.circleRadius, d.target.x))
                    return Math.max(0, Math.min(this.width, nodeX));
                })
                .attr('y2', (d)=>{
                    let nodeY = Math.max(this.circleRadius, Math.min(this.height-this.circleRadius, d.target.y))
                    return Math.max(0, Math.min(this.height, nodeY));
                });

            // // for the texts on the link
            linkTexts
                .attr('x', function(d) {
                    return Math.max(0, Math.min(w,d.source.x + (d.target.x - d.source.x) / 2 - this.getBBox().width/2))
                })
                .attr('y', (d)=>{return Math.max(0, Math.min(this.height, d.source.y + (d.target.y - d.source.y)/2))});

            this.node
                .attr("transform", (d)=>{return "translate(" + Math.max(this.circleRadius, Math.min(this.width-this.circleRadius, d.x)) +
                    ", " + Math.max(this.circleRadius, Math.min(this.height-this.circleRadius, d.y)) + ")";});
            this.nodeAboveText
                .attr("x", function (d) {return Math.max(radius/2, Math.min(w-radius/2, d.x - this.getBBox().width / 2))})
                .attr("y", function (d) {return Math.max(radius/2, Math.min(h-radius/2, d.y - this.getBBox().height / 2 ))});

            nodeText
                .attr("x", function (d) {
                    let nodeX = Math.max(radius, Math.min(w-radius, d.x))
                    return Math.max(0, Math.min(w, nodeX - this.getBBox().width / 2))})
                .attr("y", function (d) {
                    let nodeY = Math.max(radius, Math.min(h-radius, d.y))
                    return Math.max(0, Math.min(h, nodeY - this.getBBox().height / 2 ))});


        }
        this.simulation
            .nodes(graph.nodes)
            .on("tick", ticked.bind(this));

        this.simulation.force("link")
            .links(graph.links)
            .distance(this.linkDistance);


        this.svg.on("mousedown", (d, i, nodes)=> {
            this.backgroundClickHandler()
        })
    }


}