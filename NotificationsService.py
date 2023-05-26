class NotificationService:
    def __init__(self, redisProvider):
        self.r = redisProvider.r
