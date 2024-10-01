from faker import Faker
from faker_clickstream import ClickstreamProvider
import time

def generate_clickstream():
  """Gera um fluxo contínuo de eventos de click."""

  fake = Faker()
  fake.add_provider(ClickstreamProvider)

  while True:
    event = fake.session_clickstream(rand_session_max_size=50)
    print(event)
    time.sleep(1)  # Ajusta o tempo entre cada evento

if __name__ == "__main__":
  generate_clickstream()