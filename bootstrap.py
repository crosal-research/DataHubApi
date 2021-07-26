import uvicorn
import yaml
from yaml.loader import SafeLoader

with open("./configuration.yaml") as f:
    config = yaml.load(f, Loader=SafeLoader)

if config["environment"] == 'development':
    d = config["development"]["API"]
else:
    d = config["production"]["API"]


if __name__ == "__main__":
    uvicorn.run("main:app", **d)
