pub fn thread_create(node_id: &str) -> String {
    format!("sandbox.node.{node_id}.thread.create")
}

pub fn task_create(node_id: &str) -> String {
    format!("sandbox.node.{node_id}.task.create")
}

pub fn exec(node_id: &str) -> String {
    format!("sandbox.node.{node_id}.exec")
}

pub fn exec_cancel(node_id: &str) -> String {
    format!("sandbox.node.{node_id}.exec.cancel")
}

pub fn delete(node_id: &str) -> String {
    format!("sandbox.node.{node_id}.delete")
}
