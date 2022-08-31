from git.repo.base import Repo
from shared_utils.utils import PROJECT_DIR


def pull_rymscraper():
    repo_name = 'https://github.com/Chotom/rymscraper.git'
    Repo.clone_from(repo_name, f'{PROJECT_DIR}/data_processing/fetch/rym/rymscraper')
    print('Remember to install rymscraper pip packages and prepare selenium webdrivers.')


if __name__ == '__main__':
    pull_rymscraper()
