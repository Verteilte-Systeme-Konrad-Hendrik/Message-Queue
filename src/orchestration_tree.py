from orchestrator_objects import node
from orchestrator_objects import pool
import threading

tree_lock = threading.Lock()

class orchestration_tree:
    def __init__(self):
        self.root_pool = pool(None)
        self.nodes_per_pool = 2
        self.children_per_pool = 2


    def get_lowest_level(self):
        # traverse tree to find leaves
        current_pool_list = [self.root_pool]
        current_children_pools = self.root_pool.children
        while len(current_children_pools) > 0:
            if len(current_children_pools) > len(current_pool_list):
                current_pool_list = current_children_pools
            else:
                raise Exception("Child list shorter than parent... This is not a tree")
            next_children_pools = []
            for children_pool in current_children_pools:
                next_children_pools += children_pool.children
            current_children_pools = next_children_pools
        return current_pool_list


    def get_insert_pool(self):
        lowest_tree_level = self.get_lowest_level()
        # get minimum from level
        min_used_pool = min(lowest_tree_level, key=lambda x: len(x.members))
        # check level for space
        if len(min_used_pool.members) < self.nodes_per_pool:
            # Insert into this pool
            return min_used_pool
        else:
            # Create new level
            for lt_pool in lowest_tree_level:
                new_children = [pool(lt_pool) for _ in range(self.children_per_pool)]
                # Add children to the parent
                lt_pool.add_children(new_children)
            
            # Make recursive call to insert node
            print("Inserting recursive")
            return self.get_insert_pool()

orch_tree = None

def get_orchestration_tree():
    global orch_tree
    if orch_tree is None:
        orch_tree = orchestration_tree()
    
    return orch_tree
    