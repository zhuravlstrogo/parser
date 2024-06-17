wget https://mirror.cs.uchicago.edu/google-chrome/pool/main/g/google-chrome-stable/google-chrome-stable_114.0.5735.198-1_amd64.deb &&
sudo apt-get update && 
sudo dpkg -i google-chrome-stable_114.0.5735.198-1_amd64.deb &
yes Y | sudo apt-get install -f && 
yes Y | sudo apt install python3-pip && 
cat requirements.txt | xargs -n 1 pip install &&
pip3 install selenium

# https://askubuntu.com/questions/118749/package-system-is-broken-how-to-fix-it
