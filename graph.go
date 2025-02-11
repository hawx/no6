package no6

import (
	"slices"
)

var (
	// Any can be used with Query to specify a wildcard
	Any = "______ANY_______"
)

// Reading:
// https://neo4j.com/blog/rdf-vs-property-graphs-knowledge-graphs/
// https://www.iaria.org/conferences2018/filesDBKDA18/IztokSavnik_Tutorial_3store-arch.pdf
// https://github.com/hypermodeinc/dgraph/blob/master/paper/dgraph.pdf

type Triple struct{ Subject, Predicate, Object string }

// A Store holds a graph of nodes and edges that represent a set of (subject,
// predicate, object) triples.
type Store struct {
	nextUID uint64
	nodes   []graphNode
	edges   []graphEdge
}

// A graphNode represents a subject or an object.
type graphNode struct {
	id    uint64
	value string
}

// A graphEdge connects two [GraphNode] or a [GraphNode] and literal by a
// predicate.
type graphEdge struct {
	from, to uint64
	value    string
}

// Insert adds a new triple to the [Graph].
func (g *Store) Insert(triples ...Triple) {
	for _, triple := range triples {
		g.insertTriple(triple.Subject, triple.Predicate, triple.Object)
	}
}

func (g *Store) Get(subject, predicate string) string {
	subjectNode := g.getNodeByValue(subject)
	if subjectNode.id == 0 {
		return ""
	}

	var foundEdge graphEdge
	for _, edge := range g.edges {
		if edge.value == predicate && edge.from == subjectNode.id {
			foundEdge = edge
			break
		}
	}

	return g.getNode(foundEdge.to).value
}

func (g *Store) Query(subject, predicate, object string) []Triple {
	subjectNodes := g.nodes
	if subject != Any {
		subjectNodes = []graphNode{g.getNodeByValue(subject)}
	}

	objectNodes := g.nodes
	if object != Any {
		objectNodes = []graphNode{g.getNodeByValue(object)}
	}

	var triples []Triple
	for _, edge := range g.edges {
		if predicate != Any && edge.value != predicate {
			continue
		}

		subjectIdx := slices.IndexFunc(subjectNodes, func(node graphNode) bool {
			return edge.from == node.id
		})
		if subjectIdx == -1 {
			continue
		}

		objectIdx := slices.IndexFunc(objectNodes, func(node graphNode) bool {
			return edge.to == node.id
		})
		if objectIdx == -1 {
			continue
		}

		triples = append(triples, Triple{
			Subject:   subjectNodes[subjectIdx].value,
			Predicate: edge.value,
			Object:    objectNodes[objectIdx].value,
		})
	}

	return triples
}

func (g *Store) insertTriple(subject, predicate, object string) {
	subjectNode := g.getNodeByValue(subject)
	if subjectNode.id == 0 {
		g.nextUID++
		subjectNode.id = g.nextUID
		subjectNode.value = subject
		g.nodes = append(g.nodes, subjectNode)
	}

	objectNode := g.getNodeByValue(object)
	if objectNode.id == 0 {
		g.nextUID++
		objectNode.id = g.nextUID
		objectNode.value = object
		g.nodes = append(g.nodes, objectNode)
	}

	for _, edge := range g.edges {
		if edge.value == predicate &&
			edge.from == subjectNode.id && edge.to == objectNode.id {
			// don't duplicate existing edge
			return
		}
	}

	g.edges = append(g.edges, graphEdge{
		from:  subjectNode.id,
		to:    objectNode.id,
		value: predicate,
	})
}

func (g *Store) getNode(id uint64) graphNode {
	for _, node := range g.nodes {
		if node.id == id {
			return node
		}
	}

	return graphNode{}
}

func (g *Store) getNodeByValue(value string) graphNode {
	for _, node := range g.nodes {
		if node.value == value {
			return node
		}
	}

	return graphNode{}
}
