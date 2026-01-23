Indications à suivre:
Pour utiliser notre programme, vous devez déjà installer Python 3 et sysv_ipc sur votre machine UNIX, pour ce faire:

Dans un terminal UNIX:
sudo apt update && sudo apt install -y python3 python3-pip python3-dev build-essential
pip3 install sysv_ipc

Pour lancer notre cycle de la vie :
Dans un terminal, depuis le dossier Projet_PPC :
Lancer le fichier display_gui.py avec la commande:
python3 display_gui.py
Une interface GUI apparaît, vous n'avez plus qu'à appuyer sur init dans l'interface pour que la simulation démarre.
Si vous rencontrez une erreur merci de quitter l'interface avant de vouloir relancer une partie.
Si le cycle de la vie se termine (plus ni de proie ni de prédateur), merci d'appuyer sur le bouton pour arrêter la simulation et de quitter l'interface avant de relancer display_gui.py pour rejouer.
