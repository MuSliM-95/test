import { Alert } from 'antd';

function AuthError() {
    return (
        <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
            <Alert
                message="Ошибка доступа!"
                description="Вы ввели неверный токен! Проверьте его правильность или обратитесь в техническую поддержку!"
                type="error"
                showIcon
            />
        </div>
    );
}

export default AuthError;