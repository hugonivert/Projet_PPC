# Projet_PPC

## But du projet
implémenter les 4 processus suivant ainsi que ce qui les lient:
-> env 
-> proie
-> prédateur
-> display : visualisation

Tous les processus sont indépendants.
On utilise le socket de env pour que les processus interagissent (connexion). Par exemple si une proie souhaite se reproduire elle demande au socket :
REPRODUCTION <un-id-de-proie> et env accepte OK ou refuse REFUSE, même idée pour toutes les autres interractions : predateur mange proie, proie meurt, predateur meurt, proie mange herbe, predateur se reproduit.
On peut commencer par faire le diagramme demandé avec les relations entre les processus (ce qu'ils partagent et comment).
Ensuite on implémentera chacun des process indépendamment les uns des autres en testant si cela fonctionne avec des données en dur. Puis on pourra assembler les processus et essayer de faire un display un peu sympa avec maj en direct de ce qui se passe, des populations etc...


## Tâches faites:
Attention : le programme utilise sysv_ipc pour la message_queue et le shared_memory donc executable que sur UNIX
15/01 Léo : création d'un mémoire partagée par env, 
création d'un socket sur lequel preys et predators pourront se connecter, il y a une fonction qui lance des threads pour chaque nouvelle connexion à gérer pour que cela ne soit pas bloquant pour le code principal.
création de la message queue avec display dans env, il y a comme pour le socket un fonction qui est lancée sur un thread et qui lit en permanence ce qui se passe dans la message queue.

L’architecture repose sur des agents autonomes (proies/prédateurs) implémentés comme des processus indépendants.
Chaque agent établit une connexion TCP persistante avec le processus env, qui valide les actions (reproduction, alimentation, mort) et maintient l’état global via une mémoire partagée protégée par sémaphore.

## Tâches à faire:
On pourra faire des test en connectant un client au serveur de env et voir si en mettant des JOIN cela incrémente bien les compteurs de population
Réfléchir au fonctionnement de nos preys et prédators : energy, reproduction, death, passive,active
Réfléchir à notre format de display
Commencer à implémenter les preys et predators 

### 6️⃣ Deadlines
- [x] 17/12/2025 – Publication du projet
- [ ] 29/01/2026 23:59 – Soumission du code et rapport
- [ ] 30/01/2026 – Démonstration de 15 minutes


