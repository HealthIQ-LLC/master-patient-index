import datetime
import matplotlib.pyplot as plt
import networkx as nx
from sqlalchemy import or_
from sqlalchemy.dialects.postgresql import insert

from .data_utils import SYSTEM_USER
from .logger import version
from .model import db, Bulletin, EnterpriseGroup, EnterpriseMatch, key_gen

MATCH_THRESHOLD = 0.5
NODE_SIZE = 150
FONT_SIZE = 20
ALPHA = 0.5
WIDTH = 6
SEED = 7
AX_MARGINS = 0.08


class GraphReCursor:
    """
    Select a graph, recursively, via any provided record id, get the nodes and 
    weights in the dialect for init of GraphCursors.
    """
    def __init__(self, record_id):
        self.record_id = record_id
        self.matched_records = {self.record_id}
        self.graph_size = len(self.matched_records)
        self.temp_nodes_and_weights = list()
        self.already_matched = list()
        self.nodes_and_weights = list()
        self.recursive_matches = None
        self.recursive_match_graphing()

    def recursive_match_graphing(self):
        response = list()
        matched_records = self.matched_records
        graph_size = self.graph_size
        for record_id in matched_records:
            if record_id not in self.already_matched:
                query = db.session.query(EnterpriseMatch).\
                    filter(
                    EnterpriseMatch.is_valid is True,
                    or_(
                        EnterpriseMatch.record_id_low == record_id,
                        EnterpriseMatch.record_id_high == record_id,
                        )
                )
                for row in query.all():
                    a = row.record_id_low
                    b = row.record_id_high
                    weight = row.match_weight
                    self.temp_nodes_and_weights.append((a, b, weight))
                    if weight >= MATCH_THRESHOLD:
                        response.append(a)
                        response.append(b)
                self.already_matched.append(record_id)
        response_set = set(response)
        for item in response_set:
            matched_records.add(item)
        new_graph_size = len(matched_records)
        if new_graph_size > graph_size:
            self.matched_records = matched_records
            self.graph_size = new_graph_size
            self.recursive_match_graphing()
        else:
            nodes_and_weights = list()
            for tup in self.temp_nodes_and_weights:
                if tup not in nodes_and_weights:
                    nodes_and_weights.append(tup)
            self.nodes_and_weights = nodes_and_weights
            self.recursive_matches = matched_records


class GraphCursor:
    """
    The GraphCursor takes `nodes_and_weights`, a list of tups of 
    (a: int, b: int, weight: float) and batch_id, proc_id
    """
    def __init__(
            self,
            nodes_and_weights: list,
            batch_id,
            proc_id,
            match_threshold=MATCH_THRESHOLD,
            node_size=NODE_SIZE,
            font_size=FONT_SIZE,
            alpha=ALPHA,
            width=WIDTH,
            seed=SEED,
            ax_margins=AX_MARGINS
    ):
        self.batch_id = batch_id
        self.proc_id = proc_id
        self.nodes_and_weights = nodes_and_weights
        self.enterprise_id, self.graph = self.arrange_enterprise_graph()
        self.edge_labels = None
        self.config = {
            "nodes_and_weights": nodes_and_weights,
            "match_threshold": match_threshold,
            "node_size": node_size,
            "font_size": font_size,
            "alpha": alpha,
            "width": width,
            "seed": seed,
            "ax_margins": ax_margins
        }
        self.plot = self.plot_enterprise_graph()
        self.match_count = 0
        self.new_matches = list()
        self.new_groups = list()
        for weight in self.edge_labels.values():
            if weight >= self.config.get("match_threshold"):
                self.match_count += 1

    def plot_enterprise_graph(self):
        graph = self.graph
        match_threshold = self.config.get("match_threshold")
        node_size = self.config.get("node_size")
        width = self.config.get("width")
        alpha = self.config.get("alpha")
        font_size = self.config.get("font_size")
        elarge = [(u, v) for (u, v, d) in graph.edges(data=True) if d["weight"] >= match_threshold]
        esmall = [(u, v) for (u, v, d) in graph.edges(data=True) if d["weight"] < match_threshold]
        pos = nx.spring_layout(graph, seed=self.config.get("seed"))
        nx.draw_networkx_nodes(graph, pos, node_size=node_size)
        nx.draw_networkx_edges(graph, pos, edgelist=elarge, 
        	width=width, edge_color="g")
        nx.draw_networkx_edges(graph, pos, edgelist=esmall, width=width, 
        	alpha=alpha, edge_color="r", style="dashed")
        nx.draw_networkx_labels(graph, pos, font_size=font_size, 
        	font_family="sans-serif")
        edge_labels = nx.get_edge_attributes(graph, "weight")
        nx.draw_networkx_edge_labels(graph, pos, edge_labels)
        ax = plt.gca()
        ax.margins(self.config["ax_margins"])
        plt.axis("off")
        plt.tight_layout()
        self.edge_labels = edge_labels
        
        return plt

    def arrange_enterprise_graph(self):
        graph = nx.Graph()
        temp_id = None
        for a, b, weight in self.nodes_and_weights:
            if a != b:
                graph.add_edge(a, b, weight=weight)
                if temp_id is not None:
                    if a < b:
                        lower = a
                    elif b < a:
                        lower = b
                    if lower < temp_id:
                        temp_id = lower
                elif temp_id is None:
                    if a < b:
                        temp_id = a
                    elif b < a:
                        temp_id = b
        enterprise_id = temp_id

        return enterprise_id, graph

    def __call__(self):
        user = SYSTEM_USER
        transaction_key = f"{self.batch_id}_{self.proc_id}"
        for a, b, weight in self.config.get("nodes_and_weights"):
            ts = datetime.datetime.now()
            if weight >= self.config.get("match_threshold"):
                if a < b:
                    low = a
                    high = b
                else:
                    low = b
                    high = a
                etl_id = key_gen(user, version)
                staged_match_record = {
                    "etl_id": etl_id,
                    "record_id_low": low,
                    "record_id_high": high,
                    "match_weight": weight,
                    "transaction_key": transaction_key,
                    "is_valid": True,
                    "created_by": user,
                    "created_ts": ts,
                    "touched_by": user,
                    "touched_ts": ts
                }
                statement = insert(EnterpriseMatch).values(**staged_match_record).on_conflict_do_nothing()
                db.session.execute(statement)
                db.session.commit()
                self.new_matches.append(etl_id)
        group_list = list()
        for etl_id in self.new_matches:
            response = []
            query = db.session.query(EnterpriseMatch).filter(EnterpriseMatch.etl_id == etl_id)
            for row in query.all():
                response.append(row.to_dict())
            record_high = response[0].get("record_id_high")
            group_list.append(record_high)
            record_low = response[0].get("record_id_low")
            group_list.append(record_low)
        group_set = set(group_list)
        for record_id in group_set:
            ts = datetime.datetime.now()
            etl_id = key_gen(user, version)
            staged_group_record = {
                "etl_id": etl_id,
                "enterprise_id": self.enterprise_id,
                "record_id": record_id,
                "transaction_key": transaction_key,
                "created_by": user,
                "created_ts": ts
            }
            statement = insert(EnterpriseGroup).values(**staged_group_record).on_conflict_do_update(
                where=(EnterpriseGroup.c.enterprise_id != self.enterprise_id),
                set_=dict(transaction_key=transaction_key, created_ts=ts, enterprise_id=self.enterprise_id),
            )
            db.session.execute(statement)
            db.session.commit()
            query = db.session.query(EnterpriseGroup).filter("created_ts" == ts)
            # store bulletin record if necessary
            if len(query.first() == 1):  # this activity upserted a group record, so we issue the bulletin
                staged_graph_bulletin_record = {
                    "etl_id": key_gen(user, version),
                    "batch_id": self.batch_id,
                    "proc_id": self.proc_id,
                    "record_id": record_id,
                    "empi_id": self.enterprise_id,
                    "transaction_key": transaction_key,
                    "bulletin_ts": ts
                }
                statement = insert(Bulletin).values(**staged_graph_bulletin_record)
                db.session.execute(statement)
                db.session.commit()
                self.new_groups.append(etl_id)  # we are using the ETL of the group record, not of the bulletin record

    def store_graph_image(self):
        self.plot.savefig(f"{self.enterprise_id}.png")

    def __str__(self):
        return f"<GraphCursor: {self.enterprise_id} | {len(self.graph.nodes)} records | " \
               f"{len(self.graph.edges)} edges | {self.match_count} matches>"
