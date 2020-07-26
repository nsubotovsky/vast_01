
import os
import os.path
import matplotlib.pyplot as plt
import datetime
import community as community_louvain
import networkx as nx
import community as community_louvain
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd


class GraphInterface( object ):

    def __init__( self, filename ):
        self.df = pd.read_csv( os.path.join( os.getcwd(), '..', 'data', filename ) )

    def getNodesOfType( self, types=frozenset(['pers']) ):
        return { i for i in set(self.df['Source']) | set(self.df['Target']) if any( i.startswith(nodeType) for nodeType in types ) }

    def getEdgesOfType(self, types=frozenset(['0_emails', '1_phone']), scopeNodes=None):
        edges = self.df[self.df['eType'].isin(types)][['Source', 'Target']]
        edgesAsTuples = {(i['Source'], i['Target']) for _, i in edges.iterrows()}

        if scopeNodes:
            edgesAsTuples = { (source, target) for source,target in edgesAsTuples if source in scopeNodes and target in scopeNodes }

        return edgesAsTuples

    def getConnectedNodesOnly(self, nodes, links):
        linkedNodes = { l[0] for l in links } | { l[1] for l in links }
        return { node for node in nodes if node in linkedNodes }

    def buildGraph(self):
        allNodes = self.getNodesOfType({'pers'})
        edges = self.getEdgesOfType( scopeNodes=allNodes )
        usedNodes = self.getConnectedNodesOnly( allNodes, edges )

        g = nx.Graph()
        g.add_nodes_from(usedNodes)
        g.add_edges_from(edges)

        return g

    def sortNodesByCommunitySize(self):
        graph = self.buildGraph()
        community_louvain.best_partition( graph )
        allNodes = self.getNodesOfType({'pers'})

        








def main():
    return GraphInterface('Q1-Graph4.fix.csv').sortNodesByCommunitySize()

    g = GraphInterface('Q1-Graph4.fix.csv').buildGraph()


    # compute the best partition
    partition = community_louvain.best_partition(g)
    pos = nx.spring_layout(g)
    cmap = cm.get_cmap('viridis', max(partition.values()) + 1)
    nx.draw_networkx_nodes(g, pos, partition.keys(), node_size=40,cmap=cmap, node_color=list(partition.values()))

    nx.draw_networkx_edges(g, pos, alpha=0.5)
    plt.show()

if __name__ == '__main__':
    print( main() )
