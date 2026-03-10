pub(crate) mod handler;
pub(crate) mod state;

use std::{collections::HashMap, sync::Arc};

use crate::{handler::handler, state::St};
use tokio::sync::Mutex;
#[tokio::main]
async fn main() {
    // Initialiser tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:7878")
        .await
        .unwrap();
    let st: St = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let st_clone = st.clone();

        tokio::spawn(async move {
            handler(socket, st_clone).await;
        });
    }

    // TODO: Implémenter le serveur MiniRedis sur 127.0.0.1:7878
    //
    // Étapes suggérées :
    // 1. Créer le store partagé (Arc<Mutex<HashMap<String, ...>>>)
    // 2. Bind un TcpListener sur 127.0.0.1:7878
    // 3. Accept loop : pour chaque connexion, spawn une tâche
    // 4. Dans chaque tâche : lire les requêtes JSON ligne par ligne,
    //    traiter la commande, envoyer la réponse JSON + '\n'
}
