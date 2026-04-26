pub fn thread_key(deployment_id: &str, thread_id: &str) -> String {
    format!("thread/{deployment_id}/{thread_id}")
}

pub fn task_key(deployment_id: &str, project_id: &str, task_key: &str) -> String {
    format!("task/{deployment_id}/{project_id}/{task_key}")
}

pub fn session_key(sandbox_id: &str) -> String {
    format!("session/{sandbox_id}")
}
