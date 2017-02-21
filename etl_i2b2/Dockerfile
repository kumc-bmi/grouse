FROM stockport/luigi-taskrunner
# based on alpine and python3
# includes cx_Oracle but not sqlalchemy

# ISSUE: group maintainer such as heron-admin@kumc.edu?
MAINTAINER Dan Connolly <dconnolly@kumc.edu>

USER app

WORKDIR /luigi/tasks
ADD . .
RUN bash -c "source /luigi/.pyenv/bin/activate \
    && pip install -r requirements.txt"

ENTRYPOINT ["bash", "/luigi/taskrunner.sh"]
