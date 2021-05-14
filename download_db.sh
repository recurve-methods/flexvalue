if [[ $# -eq 0 ]] ; then
    echo 'You must supply which year (2020 or 2021) you want to download as an argument'
    exit 0
fi
curl --output $1.db https://storage.googleapis.com/flexvalue-public-resources/db/v1/$1.db
