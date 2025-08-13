from app.bitrix import event_bind, event_unbind
from app.settings import settings

if __name__ == "__main__":
    # отвяжите при необходимости:
    # event_unbind("onCrmDealAdd", settings.event_handler_url)
    # event_unbind("onCrmDealUpdate", settings.event_handler_url)

    print("Binding onCrmDealAdd:", event_bind("onCrmDealAdd", settings.event_handler_url))
    print("Binding onCrmDealUpdate:", event_bind("onCrmDealUpdate", settings.event_handler_url))
