pub mod node_utils {
    use std::collections::BTreeMap;

    use k8s_openapi::{
        api::{
            apps::v1::{Deployment, DeploymentSpec},
            core::v1::{Container, ContainerPort, PodSpec, PodTemplateSpec},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    };
    use kube::api::ObjectMeta;

    pub fn get_deployment_definition(
        name: &str,
        namespace: &str,
        image: &str,
        replicas: i32,
        labels: BTreeMap<String, String>,
    ) -> Deployment {
        Deployment {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(namespace.to_owned()),
                labels: Some(labels.clone()),
                ..ObjectMeta::default()
            },
            spec: Some(DeploymentSpec {
                replicas: Some(replicas),
                selector: LabelSelector {
                    match_expressions: None,
                    match_labels: Some(labels.clone()),
                },
                template: PodTemplateSpec {
                    spec: Some(PodSpec {
                        containers: vec![Container {
                            name: name.to_owned(),
                            image: Some(image.to_owned()),
                            image_pull_policy: Some("Never".to_owned()),
                            ports: Some(vec![ContainerPort {
                                container_port: 8080,
                                ..ContainerPort::default()
                            }]),
                            ..Container::default()
                        }],
                        ..PodSpec::default()
                    }),
                    metadata: Some(ObjectMeta {
                        labels: Some(labels),
                        ..ObjectMeta::default()
                    }),
                },
                ..DeploymentSpec::default()
            }),
            ..Deployment::default()
        }
    }
}
