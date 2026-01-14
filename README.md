# Projet_PPC

## But du projet
implémenter les 4 processus suivant ainsi que ce qui les lient:
-> env : processus parent qui gère l'ensemble 
-> proie
-> prédateur
-> display : visualisation

Tous les processus sont des enfants de env ce qui permettra d'avoir un accès rapide en lecture dans une mémoire paratgée. 
En revanche en écriture, on utilise le socket de env pour que les processus interagissent. Par exemple si une proie souhaite se reproduire elle demande au socket :
REPRODUCTION <un-id-de-proie> et env accepte OK ou refuse REFUSE, même idée pour toutes les autres interractions : predateur mange proie, proie meurt, predateur meurt, proie mange herbe, predateur se reproduit.
On peut commencer par faire le diagramme demandé avec les relations entre les processus (ce qu'ils partagent et comment).
Ensuite on implémentera chacun des process indépendamment les uns des autres en testant si cela fonctionne avec des données en dur. Puis on pourra assembler les processus et essayer de faire un display un peu sympa.


## Checklist du projet

### 6️⃣ Deadlines
- [x] 17/12/2025 – Publication du projet
- [ ] 29/01/2026 23:59 – Soumission du code et rapport
- [ ] 30/01/2026 – Démonstration de 15 minutes


