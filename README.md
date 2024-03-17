# Projekt z Sieci

Dokumentacja

Przyjęcie inforamcji do storzenia serwera DatabaseNode. 
-tcpport - przyjmuje nr portu i inicjuje socket serwera
-record - otrzymuje informacje o wartosci key i value
-connect - adres i nr portu serwera do którego ma się podłączyć, wysyła wiadomość do danego serwera by go podpiąć 

Przyjęcie polecenia od innego serwera/klienta.
- "Connected" - wiadomość od innego serwera by go podpiąć
- "set-value" - sprawdza czy warość serwera key jest równa z wysłaną przez klienta
	-tak - zmienia value serwera i odsyła wiadomość "OK" klientowi
	-nie - wysyła zapytanie do podpiętych serwerów 
- "get-value" - sprawdza czy warość serwera key jest równa z wysłaną przez klienta
	-tak - odsyła wiadomość swojego key:value klientowi
	-nie - wysyła zapytanie do podpiętych serwerów 
- "find-key" - - sprawdza czy warość serwera key jest równa z wysłaną przez klienta
	-tak - odsyła wiadomość swojego key:value klientowi
	-nie - wysyła zapytanie do podpiętych serwerów 
- "get-max" - zapisuje w zmiennej wartość serwera i wysyła zapytanie zawierające te wartość do innych serwerów porównujące swoją wartość z otrzymaną i wysyłają dalej większą.
- "get-min" - zapisuje w zmiennej wartość serwera i wysyła zapytanie zawierające te wartość do innych serwerów porównujące swoją wartość z otrzymaną i wysyłają dalej mniejszą.
- "new-record" - zmienia key i wartość serwera na otrzymane wartości od klienta
- "terminate" - wysyła wiadomość do sąsiednich serwerów z wiadomością by usunęły go z podpiętych serwerów poczym zamyka wątek w którym działa.

-"hostToHostImTD"/"hostToHostImTU - wiadomość od sąsiedniego serwera by usunąć go z serwerów sąsiednich

-"hostToHostD"/"hostToHostU" - wiadomość od innego serwera z dołu/góry przesłana z zapytaniem pierwotnego serwera od klienta

Klient DatabaseClient podany przez prowadzącego zajęcia. Przyjmuje parametry i wysyła operacje.


Tworzenie serwerów i przykładowe zapytanie od DatabaseClient

java DatabaseNode.java -tcpport 9991 -record 11:252 
java DatabaseNode.java -tcpport 9992 -record 22:6 -connect localhost:9991 
java DatabaseNode.java -tcpport 9993 -record 33:555 -connect localhost:9992 
(klasa od prowadzącego)   java DatabaseClient.java -gateway localhost:9991 -operation get-value 33 
