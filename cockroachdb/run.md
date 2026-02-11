lein run test -n n1 --username root --ssh-private-key ~/.ssh/id_rsa --test register --nemesis parts --concurrency 10 --os debian

lein run test -n n1 -n n2 -n n3 -n n4 -n n5 --username root --ssh-private-key ~/.ssh/id_rsa --test register --nemesis parts --concurrency 10 --os debian
