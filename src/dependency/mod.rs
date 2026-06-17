use crate::models::{DependencyNode, RootCauseResult, ServiceStatus};
use std::collections::{HashMap, HashSet};

pub struct DependencyGraph {
    nodes: HashMap<String, Vec<String>>,
    statuses: HashMap<String, ServiceStatus>,
}

impl DependencyGraph {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            statuses: HashMap::new(),
        }
    }

    pub fn add_service(&mut self, name: String, dependencies: Vec<String>) {
        self.nodes.insert(name, dependencies);
    }

    pub fn update_status(&mut self, name: &str, status: ServiceStatus) {
        self.statuses.insert(name.to_string(), status);
    }

    pub fn get_status(&self, name: &str) -> ServiceStatus {
        self.statuses
            .get(name)
            .copied()
            .unwrap_or(ServiceStatus::Unknown)
    }

    pub fn build_tree(&self, root: &str) -> DependencyNode {
        let mut visited = HashSet::new();
        self.build_tree_recursive(root, &mut visited)
    }

    fn build_tree_recursive(
        &self,
        name: &str,
        visited: &mut HashSet<String>,
    ) -> DependencyNode {
        if visited.contains(name) {
            return DependencyNode {
                name: name.to_string(),
                status: self.get_status(name),
                dependencies: vec![],
            };
        }
        visited.insert(name.to_string());

        let deps = self.nodes.get(name).cloned().unwrap_or_default();

        let children = deps
            .iter()
            .map(|dep| self.build_tree_recursive(dep, visited))
            .collect();

        DependencyNode {
            name: name.to_string(),
            status: self.get_status(name),
            dependencies: children,
        }
    }

    pub fn find_root_causes(&self) -> Vec<RootCauseResult> {
        let mut depths = HashMap::new();
        let roots = self.find_roots();

        for root in &roots {
            self.compute_depths(root, 1, &mut depths, &mut HashSet::new());
        }

        let mut unhealthy_nodes = Vec::new();

        for (name, status) in &self.statuses {
            if matches!(status, ServiceStatus::Unhealthy | ServiceStatus::Degraded) {
                let is_root_cause = self.is_root_cause(name);
                let depth = depths.get(name).copied().unwrap_or(0);

                if is_root_cause {
                    unhealthy_nodes.push(RootCauseResult {
                        service: name.clone(),
                        depth,
                        status: *status,
                    });
                }
            }
        }

        unhealthy_nodes.sort_by(|a, b| {
            b.depth
                .cmp(&a.depth)
                .then_with(|| status_priority(b.status).cmp(&status_priority(a.status)))
        });

        unhealthy_nodes
    }

    fn is_root_cause(&self, name: &str) -> bool {
        let deps = self.nodes.get(name);
        if deps.is_none() {
            return true;
        }

        for dep in deps.unwrap() {
            let dep_status = self.get_status(dep);
            if matches!(dep_status, ServiceStatus::Unhealthy | ServiceStatus::Degraded) {
                return false;
            }
        }

        true
    }

    fn find_roots(&self) -> Vec<String> {
        let mut has_parent = HashSet::new();
        for deps in self.nodes.values() {
            for dep in deps {
                has_parent.insert(dep.clone());
            }
        }

        self.nodes
            .keys()
            .filter(|name| !has_parent.contains(*name))
            .cloned()
            .collect()
    }

    fn compute_depths(
        &self,
        name: &str,
        current_depth: usize,
        depths: &mut HashMap<String, usize>,
        visited: &mut HashSet<String>,
    ) {
        if visited.contains(name) {
            return;
        }
        visited.insert(name.to_string());

        depths
            .entry(name.to_string())
            .and_modify(|d| {
                if current_depth > *d {
                    *d = current_depth;
                }
            })
            .or_insert(current_depth);

        if let Some(deps) = self.nodes.get(name) {
            for dep in deps {
                self.compute_depths(dep, current_depth + 1, depths, visited);
            }
        }
    }

    pub fn all_trees(&self) -> Vec<DependencyNode> {
        let roots = self.find_roots();
        roots.iter().map(|r| self.build_tree(r)).collect()
    }
}

fn status_priority(status: ServiceStatus) -> u8 {
    match status {
        ServiceStatus::Unhealthy => 3,
        ServiceStatus::Degraded => 2,
        ServiceStatus::Unknown => 1,
        ServiceStatus::Healthy => 0,
    }
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_graph() -> DependencyGraph {
        let mut graph = DependencyGraph::new();
        graph.add_service("a".to_string(), vec!["b".to_string(), "c".to_string()]);
        graph.add_service("b".to_string(), vec!["d".to_string()]);
        graph.add_service("c".to_string(), vec!["d".to_string()]);
        graph.add_service("d".to_string(), vec![]);
        graph
    }

    #[test]
    fn test_build_tree() {
        let graph = setup_graph();
        let tree = graph.build_tree("a");

        assert_eq!(tree.name, "a");
        assert_eq!(tree.dependencies.len(), 2);
        assert_eq!(tree.dependencies[0].name, "b");
        assert_eq!(tree.dependencies[1].name, "c");
        assert_eq!(tree.dependencies[0].dependencies.len(), 1);
        assert_eq!(tree.dependencies[0].dependencies[0].name, "d");
    }

    #[test]
    fn test_build_tree_handles_cycles() {
        let mut graph = DependencyGraph::new();
        graph.add_service("a".to_string(), vec!["b".to_string()]);
        graph.add_service("b".to_string(), vec!["a".to_string()]);

        let tree = graph.build_tree("a");
        assert_eq!(tree.name, "a");
        assert_eq!(tree.dependencies.len(), 1);
        assert_eq!(tree.dependencies[0].name, "b");
        assert_eq!(tree.dependencies[0].dependencies.len(), 1);
        assert_eq!(tree.dependencies[0].dependencies[0].name, "a");
        assert_eq!(
            tree.dependencies[0].dependencies[0].dependencies.len(),
            0
        );
    }

    #[test]
    fn test_find_root_causes() {
        let mut graph = setup_graph();
        graph.update_status("a", ServiceStatus::Unhealthy);
        graph.update_status("b", ServiceStatus::Unhealthy);
        graph.update_status("c", ServiceStatus::Healthy);
        graph.update_status("d", ServiceStatus::Unhealthy);

        let causes = graph.find_root_causes();
        assert!(!causes.is_empty());
        assert_eq!(causes[0].service, "d");
        assert_eq!(causes[0].depth, 3);
    }

    #[test]
    fn test_find_root_causes_healthy_services_not_included() {
        let mut graph = setup_graph();
        graph.update_status("a", ServiceStatus::Healthy);
        graph.update_status("b", ServiceStatus::Healthy);
        graph.update_status("c", ServiceStatus::Healthy);
        graph.update_status("d", ServiceStatus::Healthy);

        let causes = graph.find_root_causes();
        assert!(causes.is_empty());
    }

    #[test]
    fn test_all_trees() {
        let mut graph = DependencyGraph::new();
        graph.add_service("root1".to_string(), vec!["child".to_string()]);
        graph.add_service("root2".to_string(), vec![]);
        graph.add_service("child".to_string(), vec![]);

        let trees = graph.all_trees();
        assert_eq!(trees.len(), 2);

        let names: Vec<_> = trees.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"root1"));
        assert!(names.contains(&"root2"));
    }

    #[test]
    fn test_is_root_cause() {
        let mut graph = setup_graph();
        graph.update_status("a", ServiceStatus::Unhealthy);
        graph.update_status("b", ServiceStatus::Unhealthy);
        graph.update_status("c", ServiceStatus::Healthy);
        graph.update_status("d", ServiceStatus::Unhealthy);

        assert!(!graph.is_root_cause("a"));
        assert!(!graph.is_root_cause("b"));
        assert!(graph.is_root_cause("d"));
    }

    #[test]
    fn test_find_roots() {
        let graph = setup_graph();
        let roots = graph.find_roots();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0], "a");
    }
}
